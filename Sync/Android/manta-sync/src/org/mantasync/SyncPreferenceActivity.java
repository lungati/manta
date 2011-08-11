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

import org.mantasync.R;
import org.mantasync.Store.Meta_Mapping;

import android.accounts.Account;
import android.content.ContentValues;
import android.database.Cursor;
import android.os.Bundle;
import android.preference.EditTextPreference;
import android.preference.ListPreference;
import android.preference.PreferenceActivity;
import android.preference.PreferenceCategory;
import android.preference.PreferenceScreen;

public class SyncPreferenceActivity extends PreferenceActivity {
	public static final String TAG = "SyncPreferenceActivity";
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		addPreferencesFromResource(R.xml.preferences);
		Account[] accounts = Util.getAccounts(this);
		CharSequence[] accountNames = new CharSequence[accounts.length];
		for (int i = 0; i < accounts.length; ++i) {
			accountNames[i] = accounts[i].name;
		}
		ListPreference lp = (ListPreference)findPreference(SyncAdapter.ACCOUNT_PREF);
		lp.setEntries(accountNames);
		lp.setEntryValues(accountNames);
		
		PreferenceScreen screen = getPreferenceScreen();
		PreferenceCategory pc = new PreferenceCategory(this);
		screen.addPreference(pc);
		pc.setTitle("Applications");
        Cursor c = getContentResolver().query(Meta_Mapping.CONTENT_URI, null, null, null, null);
        c.moveToFirst();
        while (!c.isAfterLast()) {
        	String app = null;
        	String mappedApp = null;
        	int appCol = c.getColumnIndex(Meta_Mapping.APP);
        	if (appCol != -1 && !c.isNull(appCol)) {
        		app = c.getString(appCol);
        	}
        	int mappedAppCol = c.getColumnIndex(Meta_Mapping.MAPPED_APP);
        	if (mappedAppCol != -1 && !c.isNull(mappedAppCol)) {
        		mappedApp = c.getString(mappedAppCol);
        	}
        	if (app != null && mappedApp != null) {
        		EditTextPreference et = createMappedAppPreference(app, mappedApp);
        		pc.addPreference(et);
        	}
        	c.moveToNext();
        }
        c.close();
	}
	
	private EditTextPreference createMappedAppPreference(final String app, final String mappedApp) {
		
		EditTextPreference et = new EditTextPreference(this) {
			@Override
			protected boolean persistString(String value) {
	    	    ContentValues values = new ContentValues();
	    	    values.put(Meta_Mapping.MAPPED_APP, value.toString());
	    	    getContentResolver().update(Meta_Mapping.CONTENT_URI.buildUpon().appendEncodedPath(app).build(), values, null, null);
				return true;
			}
		};
		et.setTitle(app);
		et.setSummary("Select the organization mapping.");
		et.setDialogTitle("Select Organization");
		et.setDialogMessage("Enter the organization name to use with this application:");
		et.setKey("");
		et.setDefaultValue(mappedApp);
		return et;
	}
	
	@Override
	protected void onStop() {
		super.onStop();
		
		// Ensure sync preferences take effect.
		Util.enableGoogleAccountsForSync(this);
	}
}
