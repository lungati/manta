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
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.ProtocolException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.mantasync.Store.Base;
import org.mantasync.Store.Meta_Mapping;
import org.mantasync.Store.Meta_Table;
import org.mantasync.StoreProvider.UploadData;

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

    public static final String EXTRAS_SYNC_IS_PERIODIC = "MantaSync.periodic";

    static final String HOSTNAME_PREF = "hostname";
    static final String AUTH_TOKEN_PREF = "auth_token";
    static final String SYNC_AUTOMATICALLY_PREF = "sync_automatically";
    static final String SYNC_FREQUENCY_PREF = "sync_frequency";
    
    private static final String DEFAULT_AUTH_TOKEN = "";
    public static final boolean DEFAULT_SYNC_AUTOMATICALLY = false;
    public static final int DEFAULT_SYNC_FREQUENCY = 24 * 60 * 60; // 1 day

    private static final String AUTH_TOKEN_HEADER = "Auth-Token";
    private static final String NUM_RESULTS_HEADER = "X-Num-Results";
    
    private static final long DATE_WINDOW_OVERLAP_SECONDS = 60 * 60; // 1 hour
    
	private final Context mContext;
	private final ObjectMapper mObjectMapper;
	
	private static long sLastCompletedSync = 0;

	public String getDefaultURL() {
		if (Build.FINGERPRINT.startsWith("generic")) {
			return "http://10.0.2.2:8080/";
        } else {
        	return mContext.getString(R.string.default_url);
        }
	}
	
    public SyncAdapter(Context context, boolean autoInitialize) {
        super(context, autoInitialize);
        mContext = context;
        mObjectMapper = new ObjectMapper();
        
        SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(mContext);
        
        // Hack: In an earlier version of this app, hostname was a naked host value, 
        // rather than a URL. Fix this by clearing the value on app startup in this case.
        if (settings.getString(HOSTNAME_PREF, "").length() > 0 &&
        	!settings.getString(HOSTNAME_PREF, "").contains("http")) {
            SharedPreferences.Editor editor = settings.edit();
            Log.e(TAG, "Clearing hostname, as it does not contain http");
            editor.putString(HOSTNAME_PREF, "");
            editor.commit();
        }
        
        if (settings.getString(HOSTNAME_PREF, "").length() == 0) {
            SharedPreferences.Editor editor = settings.edit();
            String hostname = getDefaultURL();
            Log.e(TAG, "Setting default hostname to: " + hostname);
            editor.putString(HOSTNAME_PREF, hostname);
            editor.commit();
        }
        if (settings.getString(AUTH_TOKEN_PREF, "").length() == 0) {
            SharedPreferences.Editor editor = settings.edit();
            editor.putString(AUTH_TOKEN_PREF, DEFAULT_AUTH_TOKEN);
            editor.commit();
        }
        if (!settings.contains(SYNC_AUTOMATICALLY_PREF)) {
            SharedPreferences.Editor editor = settings.edit();
            editor.putBoolean(SYNC_AUTOMATICALLY_PREF, DEFAULT_SYNC_AUTOMATICALLY);
            editor.commit();
        }
        if (!settings.contains(SYNC_FREQUENCY_PREF)) {
            SharedPreferences.Editor editor = settings.edit();
            editor.putString(SYNC_FREQUENCY_PREF, String.valueOf(DEFAULT_SYNC_FREQUENCY));
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
		boolean uploadOnly = extras.getBoolean(ContentResolver.SYNC_EXTRAS_UPLOAD, false);
		boolean isPeriodic = extras.getBoolean(EXTRAS_SYNC_IS_PERIODIC, false);
		
        SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(mContext);
		
		// We only support manual or override syncs right now.
		boolean isAutomatic = false;
		if (manual || ignoreSettings) {
			Log.i(TAG, "Starting a MANUAL sync");
			// Simplify logic in this case
			manual = true;
		} else if (isPeriodic) {
			Log.i(TAG, "Starting a scheduled PERIODIC sync");
		} else {
			Log.i(TAG, "Starting an AUTOMATIC sync from a network tickle");
			isAutomatic = true;
		}

		if (isAutomatic) {
			boolean syncAutomaticallyEnabled = settings.getBoolean(SYNC_AUTOMATICALLY_PREF, DEFAULT_SYNC_AUTOMATICALLY);
			if (!syncAutomaticallyEnabled) {
				Log.e(TAG, "An AUTOMATIC sync was requested, but user has not enabled network tickle syncs. Ignoring.");
				return;
			}
		}
		
		Date now = new Date();
		if (sLastCompletedSync > 0 && now.getTime() - sLastCompletedSync < 10000) {
			// If the last sync completed 10 seconds ago, ignore this request anyway.
			Log.e(TAG, "Sync was CANCELLED because a sync completed within the past 10 seconds.");
			return;
		}
		
		//Debug.startMethodTracing("mantasync-" + now.getTime());
		
		StoreProvider localProvider = (StoreProvider)provider.getLocalContentProvider();

        String url = settings.getString(HOSTNAME_PREF, getDefaultURL());

        // TODO Is there a way to not sync everything, and instead only sync the client view? 
        // Perhaps using the extras Bundle.
        Cursor c = localProvider.query(Meta_Table.CONTENT_URI, null, null, null, null);

        // First, clear the sync status for everything.
        c.moveToFirst();
        while (!c.isAfterLast()) {
        	String pathQuery = c.getString(c.getColumnIndex(Meta_Table.PATH_QUERY));
        	ContentValues values = new ContentValues();
    		values.put(Meta_Table.SYNC_ACTIVE, false);
    		values.put(Meta_Table.PROGRESS_PERCENT, 0);
    		values.put(Meta_Table.STATUS, "Idle");
    		localProvider.update(Uri.parse(Meta_Table.CONTENT_URI.toString() + pathQuery), values, null, null);
        	c.moveToNext();
        }
        
        // Then start the sync.
        c.moveToFirst();
        while (!c.isAfterLast()) {
        	Log.e(TAG, "Syncing kind: " + c.getString(c.getColumnIndex(Meta_Table.PATH_QUERY)));
        	syncOneKind(localProvider, url, c.getString(c.getColumnIndex(Meta_Table.PATH_QUERY)),
        			c.getLong(c.getColumnIndex(Meta_Table.LAST_SYNCED)), uploadOnly);
        	c.moveToNext();
        }
        c.close();
        
        sLastCompletedSync = (new Date()).getTime();

		//Debug.stopMethodTracing();
	}
	public void syncOneKind(StoreProvider localProvider, String url, String pathQuery, long lastSynced,
			boolean uploadOnly) {
		SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(mContext);
		
		Uri tableUri = Uri.parse(Meta_Table.CONTENT_URI.toString() + pathQuery);
		List<String> path = tableUri.getPathSegments();
		if (path.size() < 2) {
			Log.e(TAG, "Invalid URI found in sync table: " + tableUri);
			return;
		}
		String app = path.get(path.size() - 2);
		String mappedApp = app;
		String kind = path.get(path.size() - 1);
		String urlBase = url;
		ContentValues values = new ContentValues();
		values.put(Meta_Table.SYNC_ACTIVE, true);
		values.put(Meta_Table.PROGRESS_PERCENT, -1);
		values.put(Meta_Table.STATUS, "Initializing");
        localProvider.insert(tableUri, null);
        localProvider.update(tableUri, values, null, null);

        Uri mappingUri = Uri.withAppendedPath(Meta_Mapping.CONTENT_URI, app);
        Cursor c = localProvider.query(mappingUri, null, null, null, null);
        if (c.moveToFirst()) {
        	int mappedAppCol = c.getColumnIndex(Meta_Mapping.MAPPED_APP);
        	if (mappedAppCol != -1 && !c.isNull(mappedAppCol)) {
        		mappedApp = c.getString(mappedAppCol);
        	}
        	int mappedUrlCol = c.getColumnIndex(Meta_Mapping.MAPPED_URL);
        	if (mappedUrlCol != -1 && !c.isNull(mappedUrlCol)) {
        		urlBase = c.getString(mappedUrlCol);
        	}
        }
        c.close();
        
        if (urlBase.endsWith("/") && pathQuery.startsWith("/")) {
        	urlBase = urlBase.substring(0, urlBase.length() - 1);
        }
		Uri destUrl = Uri.parse(urlBase + pathQuery);
		ArrayList<String> destPath = new ArrayList<String>(destUrl.getPathSegments());
		destPath.set(destPath.size() - 2, mappedApp);
		Uri.Builder builder = destUrl.buildUpon().path("");
		for (String p : destPath) {
			builder.appendPath(p);
		}
		destUrl = builder.build();

		Uri dataUri = Uri.parse(Base.CONTENT_URI_BASE.toString() + pathQuery);
        
        // ------------------- Upload -------------------
        
		values.clear();
		values.put(Meta_Table.SYNC_ACTIVE, true);
		values.put(Meta_Table.PROGRESS_PERCENT, -1);
		values.put(Meta_Table.STATUS, "Finding Changes");
        localProvider.update(tableUri, values, null, null);
		
        UploadData upload = localProvider.startUploadTransactionForKind(app, kind, dataUri);
        boolean error = upload.error;
        int errorResponseCode = -1;
        String errorMessage = "";
        Log.e(TAG, "For Kind " + kind + ":\n" + upload.data);
        if (!error && upload.count > 0) {
            // Actually upload the changes to the remote server
        	
        	values.clear();
        	values.put(Meta_Table.SYNC_ACTIVE, true);
    		values.put(Meta_Table.PROGRESS_PERCENT, -1);
    		values.put(Meta_Table.STATUS, "Uploading");
            localProvider.update(tableUri, values, null, null);
        	
    		Log.e(TAG, "Contacting hostname: " + destUrl);
    		URL uploadUrl = null;
    		String uploadResult = "";
    		
    		try {
    			uploadUrl = new URL(destUrl.toString());
    		} catch (MalformedURLException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    			error = true;
    		}
    		HttpURLConnection conn = null;
    		try {
    			conn = (HttpURLConnection)uploadUrl.openConnection();
    		} catch (IOException e) {
    			// TODO Auto-generated catch block
    			e.printStackTrace();
    			error = true;
    		}
    		try {
				conn.setRequestMethod("POST");
			} catch (ProtocolException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
    			error = true;
			}
    		conn.setConnectTimeout(5000);
    		conn.addRequestProperty(AUTH_TOKEN_HEADER, settings.getString(AUTH_TOKEN_PREF, DEFAULT_AUTH_TOKEN));
    		//conn.addRequestProperty("Cookie",G.getAuthCookie().getName() + "=" + G.getAuthCookie().getValue());
    		
    		conn.setDoOutput(true);
    		BufferedWriter wr;
			try {
				wr = new BufferedWriter(new OutputStreamWriter(conn.getOutputStream()), 8192);
	    		if (wr != null) {
	    			wr.write(upload.data);
	    		}
	    		wr.close();
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
    			error = true;
			}
    		
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
    			error = true;
    		}
    		uploadResult = sb.toString();

            Log.e(TAG, "Got uploadResult: " + uploadResult + ", upload.count=" + upload.count);
            
        	values.clear();
        	values.put(Meta_Table.SYNC_ACTIVE, true);
    		values.put(Meta_Table.PROGRESS_PERCENT, -1);
    		values.put(Meta_Table.STATUS, "Clearing Changes");
            localProvider.update(tableUri, values, null, null);
            
            if (error) {
            	try {
					errorResponseCode = conn.getResponseCode();
	            	errorMessage = conn.getResponseMessage();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
            }
        }
        
        localProvider.finishUploadTransactionForKind(app, kind, dataUri, upload, error);

        values.clear();
        if (uploadOnly) {
        	values.put(Meta_Table.SYNC_ACTIVE, false);

    		if (!error) {
    			values.put(Meta_Table.STATUS, "Idle");
    			values.put(Meta_Table.PROGRESS_PERCENT, 100);
    		}
        }
		if (error) {
			String message = "Error";
			String httpStatus = "";
			httpStatus = String.format("%d %s", errorResponseCode, errorMessage);
			if (httpStatus.length() > 0) {
				message += ": " + httpStatus;
			}
			values.put(Meta_Table.STATUS, message);
			values.put(Meta_Table.PROGRESS_PERCENT, 0);
		}
        localProvider.update(tableUri, values, null, null);
        
        // ---------------- End: Upload -----------------
        


        // ------------------- Download -------------------
        if (!uploadOnly) {

        	values.put(Meta_Table.SYNC_ACTIVE, true);
    		values.put(Meta_Table.PROGRESS_PERCENT, -1);
    		values.put(Meta_Table.STATUS, "Downloading");
            localProvider.insert(tableUri, null);
            localProvider.update(tableUri, values, null, null);
        	
			URL downloadUrl = null;
	        boolean downloadSuccess = false;
			long downloadNow = 0;
			
			if (lastSynced > 0) {
				String start_date = Store.sDateFormat.format(new Date((lastSynced - DATE_WINDOW_OVERLAP_SECONDS) * 1000));
				destUrl = destUrl.buildUpon().appendQueryParameter("date_start", start_date).build();
			}
			Log.e(TAG, "Contacting hostname: " + destUrl);
	
			try {
				downloadUrl = new URL(destUrl.toString());
			} catch (MalformedURLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			HttpURLConnection conn = null;
			try {
				conn = (HttpURLConnection)downloadUrl.openConnection();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			conn.setConnectTimeout(5000);
			conn.addRequestProperty(AUTH_TOKEN_HEADER, settings.getString(AUTH_TOKEN_PREF, DEFAULT_AUTH_TOKEN));
			//conn.addRequestProperty("Cookie",G.getAuthCookie().getName() + "=" + G.getAuthCookie().getValue());
			
			InputStream inputStream = null;
			try {
				inputStream = conn.getInputStream();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			int count = -1;
			String countString = conn.getHeaderField(NUM_RESULTS_HEADER);
			if (countString != null) {
				count = Integer.valueOf(countString);
			}
			
			if (inputStream != null) {
				downloadNow = conn.getDate() / 1000;
						
				values.clear();
				values.put(Meta_Table.STATUS, "Parsing");
		        localProvider.update(tableUri, values, null, null);
				
				JsonFactory f = new JsonFactory(mObjectMapper);
				JsonParser jp = null;
				try {
					jp = f.createJsonParser(inputStream);
				} catch (JsonParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		        
				if (jp != null) {
//					try {
//					if (jp1.nextToken() != JsonToken.START_ARRAY) {
//						throw new JsonParseException("Did not start with array", jp1.getCurrentLocation());
//					} 
//					while (jp1.nextToken() != JsonToken.END_ARRAY) {
//						if (jp1.getCurrentToken() != JsonToken.START_OBJECT) {
//							throw new JsonParseException("Array contains non-object", jp1.getCurrentLocation());
//						}
//						jp1.skipChildren();
//						count++;
//					}
//					jp1.close();
//					jp1 = null;
//					} catch (JsonParseException e) {
//						e.printStackTrace();
//					} catch (IOException e) {
//						e.printStackTrace();
//					}
					
					if (count == -1 || count > 0) {
						localProvider.updateAllFromJson(app, kind, dataUri, jp, count, tableUri);
					}
					try {
						jp.close();
					} catch (IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					downloadSuccess = true;
				}
				
			}
		
			values.clear();
			values.put(Meta_Table.SYNC_ACTIVE, false);
			if (downloadSuccess && downloadNow > 0) {
				values.put(Meta_Table.LAST_SYNCED, downloadNow);
				values.put(Meta_Table.STATUS, "Idle");
				values.put(Meta_Table.PROGRESS_PERCENT, 100);
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
				values.put(Meta_Table.STATUS, message);
				values.put(Meta_Table.PROGRESS_PERCENT, 0);
			}
	        localProvider.update(tableUri, values, null, null);
        
        }
        
        // --------------- End: Download ----------------
        
	}

}
