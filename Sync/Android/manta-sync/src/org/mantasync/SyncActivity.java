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

import java.util.List;


import org.mantasync.R;
import org.mantasync.Store.Meta_Table;

import android.accounts.Account;
import android.app.AlertDialog;
import android.app.Dialog;
import android.app.ListActivity;
import android.content.ContentResolver;
import android.content.DialogInterface;
import android.content.Intent;
import android.content.SharedPreferences;
import android.database.ContentObserver;
import android.database.Cursor;
import android.net.Uri;
import android.os.Bundle;
import android.os.Handler;
import android.preference.PreferenceManager;
import android.text.Editable;
import android.text.InputType;
import android.text.method.PasswordTransformationMethod;
import android.util.Log;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ProgressBar;
import android.widget.SimpleCursorAdapter;
import android.widget.SimpleCursorAdapter.ViewBinder;
import android.widget.TextView;
import android.widget.Toast;

public class SyncActivity extends ListActivity {
    private static final String TAG = "SyncActivity";

    // Menu item ids
    public static final int MENU_ITEM_PREFS = Menu.FIRST;
    public static final int MENU_ITEM_SYNC = Menu.FIRST + 1;
    
    // Dialog item ids
    public static final int DIALOG_GET_PASSWORD = 0;
    public static final int DIALOG_NO_ACCOUNT = 1;
    
    ContentObserver mContentObserver;
    Button mDone;
    Uri mURI;
	
	@Override
	protected void onCreate(Bundle savedInstanceState) {
		super.onCreate(savedInstanceState);
		
		Util.WarmUpDateParsing();
		
		if (getIntent().getData() == null) {
			// We were launched without context. Set the URI to the entire sync store.
			getIntent().setData(Store.Meta_Table.CONTENT_URI);
		}
		mURI = getIntent().getData();
		
		String initialPathPrefix = "";
		List<String> path = mURI.getPathSegments();
		if (path.size() == 2) {
			initialPathPrefix = "/" + path.get(1) + "/";
		}
		final String pathPrefix = initialPathPrefix;
        
		setContentView(R.layout.sync);
        Cursor cursor = managedQuery(mURI, null, null, null, null);
        
        final SimpleCursorAdapter adapter = new SimpleCursorAdapter(this, R.layout.sync_item, cursor,
                new String[] { Meta_Table.PATH_QUERY, Meta_Table.PROGRESS_PERCENT, Meta_Table.STATUS  }, 
                new int[] { android.R.id.text1, R.id.sync_progress, R.id.text3 } );
        setListAdapter(adapter);
        
        adapter.setViewBinder(new ViewBinder() {
			@Override
			public boolean setViewValue(View view, Cursor cursor, int columnIndex) {
				final int progressIndex = cursor.getColumnIndex(Meta_Table.PROGRESS_PERCENT);
				final int pathQueryIndex = cursor.getColumnIndex(Meta_Table.PATH_QUERY);
				if (columnIndex == progressIndex) {
					int progress = cursor.getInt(progressIndex);
					ProgressBar pb = (ProgressBar)view.findViewById(R.id.sync_progress);
					if (progress >= 0) {
						pb.setIndeterminate(false);
						pb.setProgress(progress);
					} else {
						pb.setIndeterminate(true);
					}
					return true;
				} else if (columnIndex == pathQueryIndex) {
					String pathQuery = cursor.getString(pathQueryIndex);
					if (pathQuery.startsWith(pathPrefix)) {
						pathQuery = pathQuery.substring(pathPrefix.length());
					}
					int index = pathQuery.indexOf('=');
					int queryEnd = pathQuery.indexOf('?');
					if (queryEnd != -1 && index != -1 && pathQuery.length() > index + 1 && 
							index == pathQuery.lastIndexOf('=')) {
						String first = pathQuery.substring(0, queryEnd);
						String second = pathQuery.substring(index + 1);
						pathQuery = first + ": " + second;
					}
					TextView textView = (TextView)view.findViewById(android.R.id.text1);
					textView.setText(pathQuery);
					return true;
				}
				return false;
			}
		});
        
        final boolean neededTablesInitiallyPresent = Util.neededTablesArePresent(mURI, getContentResolver());
        
        mContentObserver = new ContentObserver(new Handler()) {
        	boolean haveDisplayedComplete = false;
        	
        	@Override
        	public void onChange(boolean selfChange) {
        		adapter.changeCursor(
        				managedQuery(mURI, null, null, null, null));
        		Cursor c = adapter.getCursor();

        		boolean syncComplete = true;
        		c.moveToFirst();
        		while (!c.isAfterLast()) {
        			if (c.getInt(c.getColumnIndex(Meta_Table.SYNC_ACTIVE)) == 1) {
        				String message = "Sync progress: " + c.getString(c.getColumnIndex(Meta_Table.PATH_QUERY)) 
        					+ " " + c.getInt(c.getColumnIndex(Meta_Table.PROGRESS_PERCENT)) + "% " 
        					+ c.getString(c.getColumnIndex(Meta_Table.STATUS));
        				Log.e(TAG, message);
        			}
        			if (c.getLong(c.getColumnIndex(Meta_Table.LAST_SYNCED)) == 0) {
        				syncComplete = false;
        			}
        			c.moveToNext();
        		}
        		if (syncComplete) {
        			if (!haveDisplayedComplete && !neededTablesInitiallyPresent) {
        				Toast.makeText(SyncActivity.this, "Sync complete!", Toast.LENGTH_LONG).show();
        				haveDisplayedComplete = true;
        			}
        			mDone.setClickable(true);
        			mDone.setEnabled(true);
        		}
        	}
        };

        getContentResolver().registerContentObserver(mURI, true, mContentObserver);
        
        mDone = (Button)findViewById(R.id.sync_done_button);
        mDone.setClickable(neededTablesInitiallyPresent);
        mDone.setEnabled(neededTablesInitiallyPresent);
        mDone.setOnClickListener(new OnClickListener() {
			@Override
			public void onClick(View v) {
				onDoneClick();
			}
		});
    }
	
	@Override
	protected void onStart() {
		super.onStart();
		
		startSync();
	}
	
	protected void startSync() {
		SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(this);
        
        if (settings.getString(SyncAdapter.AUTH_TOKEN_PREF, "").length() == 0) {
        	showDialog(DIALOG_GET_PASSWORD);
        	return;
        }

		Bundle bundle = new Bundle();
		bundle.putBoolean(ContentResolver.SYNC_EXTRAS_MANUAL, true);
		bundle.putBoolean(ContentResolver.SYNC_EXTRAS_IGNORE_SETTINGS, true);
		Account[] accounts = Util.getAccounts(this);
		for (Account account : accounts) {
			if (!ContentResolver.isSyncActive(account, Store.AUTHORITY) && 
					!ContentResolver.isSyncPending(account, Store.AUTHORITY)) {
				Log.e(TAG, account.name + " CLAIMS to need sync, because one does not seem pending nor active");
			}
			Log.e(TAG, "Requesting sync for : " + account.name);
			ContentResolver.requestSync(account, Store.AUTHORITY, bundle);
		}
		if (accounts.length == 0) {
			showDialog(DIALOG_NO_ACCOUNT);
		}
	}
	
	@Override
	protected void onDestroy() {
		ContentObserver local = mContentObserver;
		mContentObserver = null;
		if (local != null) {
			getContentResolver().unregisterContentObserver(local);
		}
		super.onDestroy();
	}
	
	public void onDoneClick() {
		setResult(RESULT_OK);
		// TODO I don't know why onDestroy() is not sufficient here.
		ContentObserver local = mContentObserver;
		mContentObserver = null;
		if (local != null) {
			getContentResolver().unregisterContentObserver(local);
		}
		finish();
	}
	
    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        super.onCreateOptionsMenu(menu);
        
        menu.add(0, MENU_ITEM_PREFS, 0, R.string.menu_preferences)
        .setShortcut('4', 'p')
        .setIcon(android.R.drawable.ic_menu_preferences);
        menu.add(0, MENU_ITEM_SYNC, 0, R.string.menu_sync)
        .setShortcut('5', 's')
        .setIcon(android.R.drawable.ic_menu_share);
        
        return true;
    }
    
    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {    		
    	case MENU_ITEM_PREFS:
    		startActivity(new Intent(this, SyncPreferenceActivity.class));
    		return true;
    	case MENU_ITEM_SYNC:
    		startSync();
    		return true;
    	}
        return super.onOptionsItemSelected(item);
    }
    
    @Override
    protected Dialog onCreateDialog(int id, Bundle args) {
    	switch (id) {
    	case DIALOG_GET_PASSWORD:
    	{
    		final EditText input = new EditText(this);
    		AlertDialog.Builder builder = new AlertDialog.Builder(this);
    		builder
    	    .setTitle("Enter Password")
    	    .setMessage("Enter the security token for this application:")
    	    .setView(input)
    	    .setPositiveButton(android.R.string.ok, new DialogInterface.OnClickListener() {
    	        public void onClick(DialogInterface dialog, int whichButton) {
    	            Editable value = input.getText();
    	    		SharedPreferences settings = PreferenceManager.getDefaultSharedPreferences(SyncActivity.this);
    	            SharedPreferences.Editor editor = settings.edit();
    	    		editor.putString(SyncAdapter.AUTH_TOKEN_PREF, value.toString());
    	            editor.commit();
    	            dialog.dismiss();
    	            startSync();
    	        }
    	    }).setNegativeButton(android.R.string.cancel, new DialogInterface.OnClickListener() {
    	        public void onClick(DialogInterface dialog, int whichButton) {
    	            finish();
    	        }
    	    });
    		input.setInputType(InputType.TYPE_TEXT_VARIATION_VISIBLE_PASSWORD);
    		input.setTransformationMethod(new PasswordTransformationMethod());
    		Dialog alert = builder.create();
    		return alert;
    	}
    		
    	case DIALOG_NO_ACCOUNT:
    	{
    		AlertDialog.Builder builder = new AlertDialog.Builder(this);
    		builder
    	    .setTitle("Need Account")
    	    .setMessage("This application requires that a Google account be associated with this phone. "
    	    		+ "Please add a Google account under Settings and try again.")
    	    .setIcon(android.R.drawable.ic_dialog_alert)
    	    .setPositiveButton(android.R.string.ok, new DialogInterface.OnClickListener() {
    	        public void onClick(DialogInterface dialog, int whichButton) {
    	            dialog.dismiss();
    	            finish();
    	        }
    	    });
    		Dialog alert = builder.create();
    		return alert;
    	}
    	}
    	
    	return super.onCreateDialog(id, args);
    }
}
