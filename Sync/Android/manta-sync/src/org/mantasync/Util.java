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

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.mantasync.Store.Meta_Table;


import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.AccountManagerCallback;
import android.accounts.AccountManagerFuture;
import android.accounts.AuthenticatorException;
import android.accounts.OperationCanceledException;
import android.content.ContentResolver;
import android.content.ContentValues;
import android.content.Context;
import android.content.PeriodicSync;
import android.content.SharedPreferences;
import android.database.Cursor;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Bundle;
import android.preference.PreferenceManager;
import android.text.TextUtils;
import android.text.TextUtils.StringSplitter;
import android.util.Log;
import android.widget.Toast;

public class Util {
	private static final String TAG = "Util";
	
	public static final String GOOGLE_ACCOUNT_TYPE = "com.google";
    public static final String[] GOOGLE_ACCOUNT_REQUIRED_SYNCABILITY_FEATURES =
            new String[]{ /* "service_ah" */ };

	static public Account[] getAccounts(final Context context) {
		final AccountManager accountManager = AccountManager.get(context);
		final Account[] allGoogleAccounts = accountManager.getAccountsByType(
				Util.GOOGLE_ACCOUNT_TYPE);
		return allGoogleAccounts;
	}

	static public void enableGoogleAccountsForSync(final Context context) {
		final AccountManager accountManager = AccountManager.get(context);
		final List<Account> accountListData = new ArrayList<Account>();

		final Account[] allGoogleAccounts = accountManager.getAccountsByType(
				Util.GOOGLE_ACCOUNT_TYPE);
		accountManager.getAccountsByTypeAndFeatures(
				Util.GOOGLE_ACCOUNT_TYPE,
				Util.GOOGLE_ACCOUNT_REQUIRED_SYNCABILITY_FEATURES,
				new AccountManagerCallback<Account[]>() {
					public void run(AccountManagerFuture<Account[]> syncableAccountsFuture) {
						accountListData.clear();

						try {
							for (Account account : syncableAccountsFuture.getResult()) {
								accountListData.add(account);
							}
						} catch (AuthenticatorException e) {
							Toast.makeText(context,
									"Authenticator error, can't retrieve accounts",
									Toast.LENGTH_SHORT).show();
							Log.e(TAG, "AuthenticatorException while accessing account list.");
							e.printStackTrace();
						} catch (IOException e) {
							Toast.makeText(context,
									"IO error, can't retrieve accounts",
									Toast.LENGTH_SHORT).show();
							Log.e(TAG, "IOException while accessing account list.");
							e.printStackTrace();
						} catch (OperationCanceledException e) {
							Log.i(TAG, "Access of accounts list canceled.");
							e.printStackTrace();
						}


				        // If we don't have the desired settings, this is being called from a client application. 
				        // It's tough to access preferences from those apps, so just skip it for now-- we'll 
				        // capture them when running the actual sync provider.
				        boolean settingsPresent = context.getPackageName().startsWith("org.mantasync");
				        int syncFrequency = -1;
				        String prefsAccount = "";
				        if (settingsPresent) {
					        SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(context);
							syncFrequency = Integer.parseInt(settings.getString(SyncAdapter.SYNC_FREQUENCY_PREF, 
									String.valueOf(SyncAdapter.DEFAULT_SYNC_FREQUENCY)));
							prefsAccount = settings.getString(SyncAdapter.ACCOUNT_PREF, "");
				        }
						// Notify the sync system about account syncability.
						for (int i = 0; i < allGoogleAccounts.length; i++) {
							boolean syncable = false;
							for (int j = 0; j < accountListData.size(); j++) {
								if (allGoogleAccounts[i].equals(accountListData.get(j))) {
									syncable = true;
									break;
								}
							}
							if (!prefsAccount.equals(allGoogleAccounts[i].name)) {
								Log.e(TAG, "allGoogleAccounts[" + i + "] (" + allGoogleAccounts[i].name + ") != " + prefsAccount);
								continue;
							}
							
							Log.e(TAG, "allGoogleAccounts[" + i + "] is " + syncable + ", " 
									+ allGoogleAccounts[i].toString());
							ContentResolver.setIsSyncable(allGoogleAccounts[i],
									Store.AUTHORITY, syncable ? 1 : 0);
							
							// Remove the annoying built-in daily sync, as our settings supersede this.
							ContentResolver.removePeriodicSync(allGoogleAccounts[i],
									Store.AUTHORITY, new Bundle());
							
							if (settingsPresent) {
								Bundle bundle = new Bundle();
								bundle.putBoolean(SyncAdapter.EXTRAS_SYNC_IS_PERIODIC, true);
			
								if (syncable && syncFrequency > 0) {
									ContentResolver.addPeriodicSync(allGoogleAccounts[i],
											Store.AUTHORITY, bundle, syncFrequency);
									Log.e(TAG, "allGoogleAccounts[" + i + "] has periodic sync " + syncFrequency + ", " 
											+ allGoogleAccounts[i].toString());
									
									for (PeriodicSync p : ContentResolver.getPeriodicSyncs(allGoogleAccounts[i], Store.AUTHORITY)) {
										Log.e(TAG, "Periodic syncs: " + p.extras.keySet().toString() + " " + p.period);
									}
									
								} else {
									ContentResolver.removePeriodicSync(allGoogleAccounts[i],
											Store.AUTHORITY, bundle);
									Log.e(TAG, "allGoogleAccounts[" + i + "] has no periodic sync, " 
											+ allGoogleAccounts[i].toString());
								}
								ContentResolver.setSyncAutomatically(allGoogleAccounts[i],
										Store.AUTHORITY, syncable);
								Log.e(TAG, "allGoogleAccounts[" + i + "] has automatic sync " + 
										syncable + ", " 
										+ allGoogleAccounts[i].toString());
							}
						}
					}
				}, null);
	}

	static public void clearSyncedStatus(Uri uri, ContentResolver resolver) {
        Cursor c = resolver.query(uri, null, null, null, null);
        c.moveToFirst();
        ContentValues values = new ContentValues();
        values.put(Meta_Table.LAST_SYNCED, 0);
        while (!c.isAfterLast()) {
        	String pathQuery = c.getString(c.getColumnIndex(Meta_Table.PATH_QUERY));
        	if (c.getLong(c.getColumnIndex(Meta_Table.LAST_SYNCED)) != 0) {
        		Uri itemUri = Uri.parse(Meta_Table.CONTENT_URI + pathQuery);
        		if (resolver.update(itemUri, values, null, null) != 1) {
        			Log.e(TAG, "ERROR: Could not update synced status for: " + itemUri);
        		}
        	}
        	c.moveToNext();
        }
        c.close();
	}
	
	static public boolean neededTablesArePresent(Uri uri, ContentResolver resolver) {
		boolean syncRequired = false;
		Set<String> tablesWithoutData = new HashSet<String>();
		Set<String> tablesWithData = new HashSet<String>();
        Cursor c = resolver.query(uri, null, null, null, null);
        c.moveToFirst();
        while (!c.isAfterLast()) {
        	String pathQuery = c.getString(c.getColumnIndex(Meta_Table.PATH_QUERY));
        	if (c.getLong(c.getColumnIndex(Meta_Table.LAST_SYNCED)) == 0) {
        		Log.e(TAG, "Must sync before continuing: " + pathQuery);
        		syncRequired = true;
        	}
        	
    		pathQuery = pathQuery.length() > 0 ? pathQuery.substring(1) : pathQuery;
        	Uri dataUri = Uri.parse(Store.Base.CONTENT_URI_BASE + pathQuery)
        		.buildUpon().query("").build();
        	Cursor d = resolver.query(dataUri, new String[] { Store.Base._ID }, 
        			null, null, Store.Base._ID + " limit 1" );
        	boolean empty = !d.moveToFirst();
        	if (empty) {
        		tablesWithoutData.add(dataUri.getPath());
        	} else {
        		tablesWithData.add(dataUri.getPath());
        	}
        	d.close();
	        	
        	c.moveToNext();
        }
        c.close();
        
        for (String without : tablesWithoutData) {
        	if (!tablesWithData.contains(without)) {
        		Log.e(TAG, "No data present for table, must provide data first: " + without);
        		syncRequired = true;
        	}
        }
        
        return !syncRequired;
	}
	
	// Note: Assumes keys are never encoded.
	static Map<String, String> getQueryComponents(Uri uri) {
		String encodedQuery = uri.getEncodedQuery();
		StringSplitter splitter = new TextUtils.SimpleStringSplitter('&');
		Map<String, String> result =  new HashMap<String, String>();

		splitter.setString(encodedQuery);
		for (String s : splitter) {
			int i = s.indexOf('=');
			if (i == -1) {
				result.put(s, "");
				continue;
			}
			String key = s.substring(0, i);
			String value = (i == s.length() - 1) ? "" : s.substring(i+1, s.length());
			result.put(key, Uri.decode(value));
		}
		return result;
	}

	static private class DateWarmUpTask extends AsyncTask<String, Integer, Integer> {

		@Override
		protected Integer doInBackground(String... params) {

			SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
			try {
				format.parse(params[0]);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return 0;
		}
	}

	static void WarmUpDateParsing() {
		new DateWarmUpTask().execute("2011-06-20 13:45:46.062081");
	}

}
