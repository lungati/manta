/*******************************************************************************
 * Copyright 2011 Kevin Gibbs and The Manta Project
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/
package org.mantasync;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Date;
import java.util.List;


import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.ObjectMapper;
import org.mantasync.Store.Meta;

import android.accounts.Account;
import android.content.AbstractThreadedSyncAdapter;
import android.content.ContentProviderClient;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.SharedPreferences;
import android.content.SyncResult;
import android.database.Cursor;
import android.net.Uri;
import android.os.Build;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.util.Log;

public class SyncAdapter extends AbstractThreadedSyncAdapter {
    private static final String TAG = "Manta.SyncAdapter";
    
	public static final String GOOGLE_ACCOUNT_TYPE = "com.google";
    public static final String[] GOOGLE_ACCOUNT_REQUIRED_SYNCABILITY_FEATURES =
            new String[]{ /* "service_ah" */ };


    static final String HOSTNAME_PREF = "hostname";
    static final String AUTH_TOKEN_PREF = "auth_token";
    
    private static final String DEFAULT_HOSTNAME = "jsonsyncstore.appspot.com";
    private static final String DEFAULT_AUTH_TOKEN = "";
    
    private static final String AUTH_TOKEN_HEADER = "Auth-Token";
    
    private static final long DATE_WINDOW_OVERLAP_SECONDS = 60 * 60; // 1 hour
    
	private final Context mContext;
	private final ObjectMapper mObjectMapper;
	
	private static long sLastCompletedSync = 0;

	public static String getDefaultHostname() {
		if (Build.FINGERPRINT.startsWith("generic")) {
			return "10.0.2.2:8080";
        } else {
        	return DEFAULT_HOSTNAME;
        }
	}
	
    public SyncAdapter(Context context, boolean autoInitialize) {
        super(context, autoInitialize);
        mContext = context;
        mObjectMapper = new ObjectMapper();
        
        SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(mContext);
        
        if (settings.getString(HOSTNAME_PREF, "").length() == 0) {
            SharedPreferences.Editor editor = settings.edit();
            String hostname = getDefaultHostname();
            Log.e(TAG, "Setting default hostname to: " + hostname);
            editor.putString(HOSTNAME_PREF, hostname);
            editor.commit();
        }
        if (settings.getString(AUTH_TOKEN_PREF, "").length() == 0) {
            SharedPreferences.Editor editor = settings.edit();
            editor.putString(AUTH_TOKEN_PREF, DEFAULT_AUTH_TOKEN);
            editor.commit();
        }
    }
	
	@Override
	public void onPerformSync(Account account, Bundle extras, String authority,
			ContentProviderClient provider, SyncResult syncResult) {
		Log.e(TAG, "Sync request issued");
		// One way or another, delay follow-up syncs for another 10 minutes.
		syncResult.delayUntil = 600;
		
		boolean manual = extras.getBoolean(ContentResolver.SYNC_EXTRAS_MANUAL, false);
		boolean ignoreSettings = extras.getBoolean(ContentResolver.SYNC_EXTRAS_IGNORE_SETTINGS, false);
		// We only support manual or override syncs right now.
		if (!(manual || ignoreSettings)) {
			return;
		}
		
		Date now = new Date();
		if (sLastCompletedSync > 0 && now.getTime() - sLastCompletedSync < 10000) {
			// If the last sync completed 10 seconds ago, ignore this request anyway.
			return;
		}
		
		//Debug.startMethodTracing("mantasync-" + now.getTime());
		
		StoreProvider localProvider = (StoreProvider)provider.getLocalContentProvider();

        SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(mContext);
        String hostname = settings.getString(HOSTNAME_PREF, getDefaultHostname());

        // TODO Is there a way to not sync everything, and instead only sync the client view? 
        // Perhaps using the extras Bundle.
        Cursor c = localProvider.query(Meta.CONTENT_URI, null, null, null, null);

        // First, clear the sync status for everything.
        c.moveToFirst();
        while (!c.isAfterLast()) {
        	String pathQuery = c.getString(c.getColumnIndex(Meta.PATH_QUERY));
        	ContentValues values = new ContentValues();
    		values.put(Meta.SYNC_ACTIVE, false);
    		values.put(Meta.PROGRESS_PERCENT, 0);
    		values.put(Meta.STATUS, "Idle");
    		localProvider.update(Uri.parse(Meta.CONTENT_URI.toString() + pathQuery), values, null, null);
        	c.moveToNext();
        }
        
        // Then start the sync.
        c.moveToFirst();
        while (!c.isAfterLast()) {
        	Log.e(TAG, "Sycing kind: " + c.getString(c.getColumnIndex(Meta.PATH_QUERY)));
        	syncOneKind(localProvider, hostname, c.getString(c.getColumnIndex(Meta.PATH_QUERY)),
        			c.getLong(c.getColumnIndex(Meta.LAST_SYNCED)));
        	c.moveToNext();
        }
        c.close();
        
        sLastCompletedSync = (new Date()).getTime();

		//Debug.stopMethodTracing();
	}
	public void syncOneKind(StoreProvider localProvider, String host, String pathQuery, long lastSynced) {
		SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(mContext);
		
		Uri uri = Uri.parse(Meta.CONTENT_URI.toString() + pathQuery);
		List<String> path = uri.getPathSegments();
		if (path.size() < 2) {
			Log.e(TAG, "Invalid URI found in sync table: " + uri);
			return;
		}
		String app = path.get(path.size() - 2);
		String kind = path.get(path.size() - 1);
		ContentValues values = new ContentValues();
		values.put(Meta.SYNC_ACTIVE, true);
		values.put(Meta.PROGRESS_PERCENT, -1);
		values.put(Meta.STATUS, "Downloading");
        localProvider.insert(uri, null);
        localProvider.update(uri, values, null, null);
	
        boolean success = false;
		long now = 0;
		
		String result = "";
		Uri destUrl = Uri.parse("http://" + host + pathQuery);
		if (lastSynced > 0) {
			String start_date = Store.sDateFormat.format(new Date((lastSynced - DATE_WINDOW_OVERLAP_SECONDS) * 1000));
			destUrl = destUrl.buildUpon().appendQueryParameter("date_start", start_date).build();
		}
		URL url = null;
		Log.e(TAG, "Contacting hostname: " + destUrl);

		try {
			url = new URL(destUrl.toString());
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		HttpURLConnection conn = null;
		try {
			conn = (HttpURLConnection)url.openConnection();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		conn.setConnectTimeout(5000);
		conn.addRequestProperty(AUTH_TOKEN_HEADER, settings.getString(AUTH_TOKEN_PREF, DEFAULT_AUTH_TOKEN));
		//conn.addRequestProperty("Cookie",G.getAuthCookie().getName() + "=" + G.getAuthCookie().getValue());
		
		StringBuffer sb = new StringBuffer();
		String line;

		BufferedReader rd = null;
		try {
			rd = new BufferedReader(new InputStreamReader(conn.getInputStream()), 8192);
			if (rd != null) {
				while ((line = rd.readLine()) != null)
				{
					sb.append(line);
				}

				rd.close();
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		result = sb.toString();
		
		// TODO Switch this code to using the InputStream directly, which is more efficient. 
		// To allow parsing twice, mark the stream, run the first parser, then rewind the 
		// stream to the original mark.
		
		if (result.length() > 0) {
			now = conn.getDate() / 1000;
					
			values.clear();
			values.put(Meta.STATUS, "Parsing");
	        localProvider.update(uri, values, null, null);
			
			JsonFactory f = new JsonFactory(mObjectMapper);
			JsonParser jp1 = null;
			JsonParser jp2 = null;
			try {
				jp1 = f.createJsonParser(result);
				jp2 = f.createJsonParser(result);
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	        
			if (jp1 != null && jp2 != null) {
				int count = 0;

				try {
				if (jp1.nextToken() != JsonToken.START_ARRAY) {
					throw new JsonParseException("Did not start with array", jp1.getCurrentLocation());
				} 
				while (jp1.nextToken() != JsonToken.END_ARRAY) {
					if (jp1.getCurrentToken() != JsonToken.START_OBJECT) {
						throw new JsonParseException("Array contains non-object", jp1.getCurrentLocation());
					}
					jp1.skipChildren();
					count++;
				}
				jp1.close();
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				localProvider.updateAllFromJson(app, kind, jp2, count, uri);
				try {
					jp2.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				success = true;
			}
			
		}
		
		values.clear();
		values.put(Meta.SYNC_ACTIVE, false);
		if (success && now > 0) {
			values.put(Meta.LAST_SYNCED, now);
			values.put(Meta.STATUS, "Idle");
			values.put(Meta.PROGRESS_PERCENT, 100);
		} else {
			String message = "Error";
			String httpStatus = "";
			try {
				httpStatus = String.format("%d %s", conn.getResponseCode(), conn.getResponseMessage());
			} catch (IOException e) {
				e.printStackTrace();
			}
			if (httpStatus.length() > 0) {
				message += ": " + httpStatus;
			}
			values.put(Meta.STATUS, message);
			values.put(Meta.PROGRESS_PERCENT, 0);
		}
        localProvider.update(uri, values, null, null);
		
	}

}
