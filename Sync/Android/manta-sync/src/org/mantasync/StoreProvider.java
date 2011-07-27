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
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.JsonToken;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.util.TokenBuffer;
import org.json.JSONObject;
import org.mantasync.Store;
import org.mantasync.Store.Base;
import org.mantasync.Store.Meta_Mapping;
import org.mantasync.Store.Meta_Table;

import android.content.ContentProvider;
import android.content.ContentValues;
import android.content.Context;
import android.content.UriMatcher;
import android.database.Cursor;
import android.database.MatrixCursor;
import android.database.SQLException;
import android.database.DatabaseUtils.InsertHelper;
import android.database.sqlite.SQLiteDatabase;
import android.database.sqlite.SQLiteOpenHelper;
import android.database.sqlite.SQLiteQueryBuilder;
import android.net.Uri;
import android.text.TextUtils;
import android.util.Log;
import android.util.Pair;

public class StoreProvider extends ContentProvider {

    private static final String TAG = "Manta.StoreProvider";

    private static final String DATABASE_NAME_PREFIX = "mantastore_app_";
    private static final String DATABASE_NAME_SUFFIX = ".db";
    private static final String META_DATABASE_NAME = "mantastore_meta.db";
    private static final int DATABASE_VERSION = 5;

    private static final UriMatcher sUriMatcher;
    
    // URI types
    private static final int ITEM_LIST = 1;
    private static final int ITEM_KEY = 2;
    private static final int ITEM_TABLE = 3;
    private static final int ITEM_TABLE_LIST = 4;
    private static final int ITEM_MAPPING = 5;
    private static final int ITEM_MAPPING_LIST = 6;
    
    public enum Mode {
        /**
         * Replace rows with the provided data.
         */
        REPLACE,
        /**
         * Upsert, creating new rows or updating rows as needed.
         */
        UPSERT,
    }
	
    /**
     * This class helps open, create, and upgrade the database file.
     */
    private static class MetaDatabaseHelper extends SQLiteOpenHelper {

        MetaDatabaseHelper(Context context, StoreProvider provider) {
            super(context, META_DATABASE_NAME, null, DATABASE_VERSION);
        }

        @Override
        public void onOpen(SQLiteDatabase db) {
        	super.onOpen(db);
        	db.setLockingEnabled(false);
        }
        
        @Override
        public void onCreate(SQLiteDatabase db) {
        	db.execSQL("CREATE TABLE '" + Meta_Table.TABLE_NAME + "' ("
        			+ Meta_Table._ID + " INTEGER PRIMARY KEY, "
                    + Meta_Table.PATH_QUERY + " TEXT UNIQUE, "
                    + Meta_Table.LAST_SYNCED + " INTEGER DEFAULT 0 "
                    + ");");
        	db.execSQL("CREATE TABLE '" + Meta_Mapping.TABLE_NAME + "' ("
        			+ Meta_Mapping._ID + " INTEGER PRIMARY KEY, "
                    + Meta_Mapping.APP + " TEXT UNIQUE, "
                    + Meta_Mapping.MAPPED_APP + " TEXT "
                    + Meta_Mapping.MAPPED_URL + " TEXT "
                    + ");");
        }
        
        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.w(TAG, "Upgrading database from version " + oldVersion + " to "
                    + newVersion + ", which will destroy all old data");
            db.execSQL("DROP TABLE IF EXISTS \"" + Meta_Table.TABLE_NAME + "\"");
            db.execSQL("DROP TABLE IF EXISTS \"" + Meta_Mapping.TABLE_NAME + "\"");
            onCreate(db);
        }
    }
    
    /**
     * This class helps open, create, and upgrade the database file.
     */
    private static class DatabaseHelper extends SQLiteOpenHelper {

    	HashMap<String, List<String>> mTableColumnMap;
    	Context mContext;
    	StoreProvider mProvider;

		final SimpleDateFormat mDateFormat;
		
		static boolean isValidAppString(String app) {
			return !(app.contains("/") || app.contains("..") || app.contains(" "));
		}
		
        DatabaseHelper(Context context, StoreProvider provider, String app) {
            super(context, DATABASE_NAME_PREFIX + app + DATABASE_NAME_SUFFIX, null, DATABASE_VERSION);
            mContext = context;
            mProvider = provider;
            mDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        }

        @Override
        public void onOpen(SQLiteDatabase db) {
        	super.onOpen(db);
        	db.setLockingEnabled(false);
        	// Experimental high-performance options
        	//Log.e(TAG, "Adding performance PRAGMAS");
        	//db.execSQL("PRAGMA synchronous = 0;");
        	//db.rawQuery("PRAGMA journal_mode = MEMORY;", null);
        }
        
        @Override
        public void onCreate(SQLiteDatabase db) {
            // TODO possibly create ID table here?
        }
        
        public HashMap<String, List<String>> getTableColumnMap(SQLiteDatabase db) {
        	if (mTableColumnMap == null) {
                mTableColumnMap = new HashMap<String, List<String>>();
        		populateTableColumnMap(db);
        	}
        	return mTableColumnMap;
        }
        
        public void populateTableColumnMap(SQLiteDatabase db) {
        	// Populate list of tables
        	Cursor cur = db.rawQuery("SELECT name FROM sqlite_master WHERE type='table'", null);
        	if (cur != null) {
        		cur.moveToFirst();
                while (cur.isAfterLast() == false) {
                	String table = cur.getString(0);
                	if (!table.equals("android_metadata")) {
	                    List<String> columns = new ArrayList<String>();
	                    getTableColumnMap(db).put(table, columns);
	                    {
	                    	// TODO escape table to prevent SQL insertion attack
	                    	Cursor cur2 = db.rawQuery("PRAGMA table_info( '" + table + "' )", null);
	                    	if (cur2 != null) {
	                    		cur2.moveToFirst();
	                    		while (cur2.isAfterLast() == false) {
	                    			String column = cur2.getString(1);
	                    			columns.add(column);
	                    			cur2.moveToNext();
	                    		}
	                    		cur2.close();
	                    	}
	                    }
                	}
               	    cur.moveToNext();
                }
                cur.close();
        	}
        }
        
        public List<String> getOrCreateKindTable(SQLiteDatabase db, String app, String kind) {
        	// TODO make use of app
        	List<String> columns = getTableColumnMap(db).get(kind);
        	if (columns == null) {
        		// TODO escape table to prevent SQL insertion attack
	            db.execSQL("CREATE TABLE '" + kind + "' ("
	                    + Base.KEY + " TEXT PRIMARY KEY,"
	                    + Base.REV + " TEXT,"
	                    + Base.DATE + " INTEGER,"
	                    + Base.DIRTY + " INTEGER DEFAULT 0,"
	                    + Base.CHANGES + " TEXT"
	                    + ");");
	            columns = new ArrayList<String>();
	            getTableColumnMap(db).put(kind, columns);
	            Collections.addAll(columns, new String[] {Base.KEY, Base.REV, Base.DATE, Base.DIRTY, Base.CHANGES});
        	}
        	return columns;
        }
        
        public void createColumn(SQLiteDatabase db, String app, String kind, String name) {
        	List<String> columns = getOrCreateKindTable(db, app, kind);
        	if (!name.equals("*") && !columns.contains(name)) {
	        	// TODO protect against SQL injection attack
        		Log.e(TAG, "Inserting new column");
	        	db.execSQL("ALTER TABLE '" + kind + "' ADD COLUMN '" + name + "';");
	        	columns.add(name);
        	}
        }

        public String findJoinColumn(SQLiteDatabase db, String app, String kind1, String kind2) {
        	List<String> columns1 = getOrCreateKindTable(db, app, kind1);
        	List<String> columns2 = getOrCreateKindTable(db, app, kind2);
        	if (columns1 == null || columns2 == null) {
        		return null;
        	}
        	// TODO This could be memoized for performance
        	List<String> joinColumns = new ArrayList<String>();
        	List<String> idColumns = new ArrayList<String>();
        	for (int i = 0; i < columns2.size(); ++i) {
        		String col = columns2.get(i);
        		if (col.equals(Base.TYPE) || col.equals(Base.KEY) || col.equals(Base.DATE) || col.equals(Base.REV) || col.equals(Base.DIRTY)) {
        			continue;
        		}
        		if (columns1.contains(col)) {
        			// TODO improvement: find all columns. if there is exactly one match, continue. 
        			// if there is exactly one match containing ID, continue. otherwise, do NOT provide 
        			// join, require client to provide in where block.
        			joinColumns.add(col);
        			if (col.endsWith("id")) {
        				idColumns.add(col);
        			}
        		}
        	}
        	Collections.sort(joinColumns);
        	Collections.sort(idColumns);
        	if (joinColumns.size() == 1) {
        		return joinColumns.get(0);
        	}
        	if (idColumns.size() == 1) {
        		return idColumns.get(0);
        	}
        	if (idColumns.size() > 1) {
        		return idColumns.get(0);
        	}
        	if (joinColumns.size() > 1) {
        		return joinColumns.get(0);
        	}
        	return null;
        }
        
        @SuppressWarnings("unchecked")
		public static Map<String, Object> getNextNewObject(Map<String, Pair<String, Integer>> presentRevs, 
        													JsonParser jp) {
			Map<String, Object> object = null;
        	try {
				while (jp.nextToken() != JsonToken.END_ARRAY) {
					if (jp.getCurrentToken() != JsonToken.START_OBJECT) {
						throw new JsonParseException("Array contains non-object", jp.getCurrentLocation());
					}
					TokenBuffer buffer = new TokenBuffer(jp.getCodec());
					buffer.copyCurrentStructure(jp);
					JsonParser keyJp = buffer.asParser();
					String key = null;
					String rev = null;
					keyJp.nextToken();
					while (keyJp.nextToken() != JsonToken.END_OBJECT) {
						String name = keyJp.getCurrentName();
						keyJp.nextToken();
						if (Base.KEY.equals(name)) {
							key = keyJp.getText();
						} else if (Base.REV.equals(rev)) {
							rev = keyJp.getText();
						}
					}
					Pair<String,Integer> entry = presentRevs.get(key);
					if (entry != null && entry.first.equals(rev)) {
						// We can skip this entity.
						continue;
					}
					
					JsonParser objectJp = buffer.asParser();
					object = objectJp.readValueAs(Map.class);
					break;
				}
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonProcessingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return object;
        }
        
        public void insertAllFromJson(SQLiteDatabase db, String app, String kind, JsonParser jp, int count, Uri metaUpdateUri, Mode mode) {
            String kindQuoted = "'" + kind + "'";
        	ContentValues values = new ContentValues();
        	
        	values.clear();
    		values.put(Meta_Table.STATUS, "Looking up existing entities");
            mProvider.update(metaUpdateUri, values, null, null);
        	
			Map<String, Pair<String, Integer>> presentRevs = new HashMap<String, Pair<String, Integer>>();
            List<String> cols = getTableColumnMap(db).get(kind);
            if (cols != null && cols.contains(Base.KEY) && cols.contains(Base.REV)) {
	    		// Find all existing entities. Eliminate entities that we do not need to consider (no change).
				Cursor cur = db.query(kindQuoted, new String[] { Base.KEY, Base.REV, "rowid" }, null, null, null, null, null);
				cur.moveToFirst();
				while (!cur.isAfterLast()) {
					presentRevs.put(cur.getString(0), new Pair<String, Integer>(cur.getString(1), cur.getInt(2)));
					cur.moveToNext();
				}
	    		cur.close();
            }
            
    		values.clear();
    		values.put(Meta_Table.PROGRESS_PERCENT, 0);
    		values.put(Meta_Table.STATUS, "Inserting " + 0 + "/" + count);
            mProvider.update(metaUpdateUri, values, null, null);
    		
            // TODO Use app here also.
        	InsertHelper helper = new InsertHelper(db, kindQuoted);
            
    		// Then, start a transaction to do the actual updates.
            int writes = 0;
            long lastTime = System.currentTimeMillis();
            long lastWrites = writes;
            double lastRate = 0.0;
            boolean entitiesLeft = true;
            
            JsonToken token = null;
            try {
            	token = jp.nextToken();
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (token != JsonToken.START_ARRAY) {
				Log.e(TAG, "Could not parse JSON, data was not an array. Skipping parse of " + kind);

				values.clear();
				values.put(Meta_Table.PROGRESS_PERCENT, 0);
				values.put(Meta_Table.STATUS, "Error in JSON data");
			    mProvider.update(metaUpdateUri, values, null, null);
			    return;
			}
            
            while (entitiesLeft) {
	        	db.beginTransaction();
                
	        	int writesTxStart = writes;
	        	try {
	        		for (int j = 0; j < 50; ++j) {
	        			Map<String, Object> object = getNextNewObject(presentRevs, jp);
	                    if (object == null) {
	                    	entitiesLeft = false;
	                    	break;
	                    }
						if (insertFromJson(db, app, kind, object, mode, presentRevs, helper)) {
							writes++;
							long now = System.currentTimeMillis();
							long elapsed = now - lastTime;
							if (elapsed > 2000) {
								lastRate = ((double)(writes - lastWrites) / ((double)elapsed / 1000.0));
								Log.i(TAG, "Write rate: " + lastRate + " w/s");
								lastTime = now;
								lastWrites = writes;
							}
						}
	        		}

					values.clear();
		    		values.put(Meta_Table.PROGRESS_PERCENT, (int)((writes / (float)count) * 100));
		    		values.put(Meta_Table.STATUS, "Inserting " + writes + "/" + count + ", " + String.format("%.1f", lastRate) + " writes/sec");
		            mProvider.update(metaUpdateUri, values, null, null);
		            
	        	    db.setTransactionSuccessful();
	        	} finally {
	        		db.endTransaction();
	        		Log.i(TAG, "Wrote " + (writes - writesTxStart) + " in last transaction");
	        	}
            }
    		helper.close();
        }

        @SuppressWarnings("unused")
		public void createColumnsFromJson(SQLiteDatabase db, String app, String kind, Map<String,Object> json) {
        	// Ensure needed columns are present
        	List<String> columns = getOrCreateKindTable(db, app, kind);
        	Iterator<String> iter = json.keySet().iterator();
        	while (iter.hasNext()) {
        		String name = iter.next();
        		if (!columns.contains(name)) {
        			createColumn(db, app, kind, name);
        		}
        	}
        }
        
        public boolean insertFromJson(SQLiteDatabase db, String app, String kind, Map<String,Object> json, Mode mode, 
        		Map<String, Pair<String, Integer>> presentRevs, InsertHelper helper) {
        	// TODO Handle deletion
        	
        	String kindQuoted = "\"" + kind + "\"";
        	int existingRowid = -1;
        	{
				String key = (String)json.get(Base.KEY);
	        	String rev = (String)json.get(Base.REV);
	        	Pair<String, Integer> entry = presentRevs.get(key);
	        	if (entry != null) {
	        		if (entry.first.equals(rev)) {
	        			// We already have this exact key and revision. We're done.
	        			return false;
	        		}
	        		// Otherwise, we at least have an existing rowid for this entry. Use it for faster access.
	        		existingRowid = entry.second;
	        	}
			} 
        	
        	List<String> columns = getOrCreateKindTable(db, app, kind);
        	Iterator<String> iter = json.keySet().iterator();
        	ContentValues values = new ContentValues();
        	boolean allNull = true;
        	while (iter.hasNext()) {
        		String name = iter.next();
        		
            	// Ensure needed columns are present
        		if (!columns.contains(name)) {
        			createColumn(db, app, kind, name);
        		}
        		
        		Object value = null;
				value = json.get(name);
				boolean wasNull = false;
        		if (value != null && (name.equals("date") || name.endsWith("date"))) {
        			// Coerce date into int as unixepoch
        			// TODO Consider just storing dates as strings, as SQLite prefers that
        			try {
        				Date date = mDateFormat.parse((String)value);
        				values.put(name, date.getTime() / 1000);
        			} catch (ParseException e) {
        				Log.e(TAG, "Could not parse date, using string: " + (String)value);
        				values.put(name, (String)value);
					}
        		}
        		else if (value instanceof Integer) {
        			values.put(name, (Integer)value);
        		} else if (value instanceof Long) {
            		values.put(name, (Long)value);
        		} else if (value instanceof Double) {
            		values.put(name, (Double)value);
        		} else if (value instanceof Boolean) {
            		values.put(name, (Boolean)value);
        		} else if (value instanceof String) {
            		values.put(name, (String)value);
        		} else if (value == JSONObject.NULL) {
        			values.putNull(name);
        			wasNull = true;
        		} else if (value == JsonToken.VALUE_NULL) {
        			values.putNull(name);
        			wasNull = true;
        		} else if (value == null) {
        			values.putNull(name);
        			wasNull = true;
        		} else {
        			// TODO support array types, mapping types, or error gracefully
        			Log.e(TAG, "Could not extract/use type from data, inserting generically");
        			values.put(name, value.toString());
        		}

        		if (allNull && !wasNull && !Store.Base.BUILT_IN_COLUMNS_LIST.contains(name)) {
        			// We have at least one non-built-in-column that has a value.
        			allNull = false;
        		}
        		
        	}
        	
        	// Insert the data
        	// TODO make use of app
        	boolean changed = false;
        	switch (mode) {
        		case REPLACE: {
        			if (db.insertWithOnConflict(kindQuoted, null, values, SQLiteDatabase.CONFLICT_REPLACE) == -1) {
        				// TODO error occurred
        				Log.e(TAG, "Could not insert entity");
        			} else {
        				changed = true;
        			}
        		}
        		case UPSERT: {
        			Cursor cur = null;
        			
        			// The following code does a safer update process, where we check for the existence of 
        			// each entity before updating or inserting. However, we've already checked for all 
        			// entities at the start of this update. Thus, for speed, we will trust that. If there 
        			// is an error, we should catch it here and use the safe method instead.
        			if (existingRowid == -1) {
        				if (allNull) {
        					// This was a requested deletion, but we've already deleted or never had the data. 
        					// We're done.
        					changed = true;
        				} else {
	        				// No existing entity, insert without a new query.
	        				// TODO I don't know if this code can handle introduction of a new column after 
	        				// the helper has been created.
	        				if (helper.insert(values) == -1) {
	            				// TODO error occurred
	            				Log.e(TAG, "Could not insert entity from initial existience "
	            						+ "query. Falling back on safer method.");	
	            			} else {
	            				changed = true;
	            			}
        				}
        			}
        			
        			// If the change did not take, run the safer method.
        			if (!changed) {
	        			if (existingRowid == -1) {
	        				cur = db.query(kindQuoted, new String[] { Base.KEY, Base.REV }, Base.KEY + " = ?", 
	        					new String[] { (String)values.get(Base.KEY) }, null, null, null);
	        			} else {
	        				// Use existing rowid for faster access.
	        				cur = db.query(kindQuoted, new String[] { Base.KEY, Base.REV }, "rowid = ?", 
	        					new String[] { String.valueOf(existingRowid) }, null, null, null);
	        			}
	        			cur.moveToFirst();
	        	        if (cur.isAfterLast() == true) {
	        	        	// No entities.
	        	        	// If we have a deletion, we're done.
	        	        	if (allNull) {
	        	        		changed = true;
	        	        	} else {
		        	        	// Otherwise, we have data and thus we have something to insert.
		        	        	if (db.insert(kindQuoted, null, values) == -1) {
		            				// TODO error occurred
		            				Log.e(TAG, "Could not insert entity");
		            			} else {
		            				changed = true;
		            			}
	        	        	}
	        	        } else {
	        	        	// Already present.
	        	        	// If this is a deletion, then we should delete now as we have real data.
	        	        	if (allNull) {
	        	        		if (db.delete(kindQuoted, Base.KEY + " = ?", 
		        	        			new String[] { (String)values.get(Base.KEY) }) != 1) {
		            				// TODO error occurred
		            				Log.e(TAG, "Could not delete entity");
		            			} else {
		            				changed = true;
		            			}
	        	        	} else {
		        	        	// We have a real update to do, so see if we have this revision.
		        	        	String revision = cur.getString(cur.getColumnIndexOrThrow(Base.REV));
		        	        	if (!revision.equals(values.getAsString(Base.REV))) {
		        	        		Log.i(TAG, "Revisions do not match, updating: " + revision + ", " + values.getAsString(Base.REV));
			        	        	// Revisions differ, perform an update.
		        	        		
			        	        	if (db.update(kindQuoted, values,  Base.KEY + " = ?", 
			        	        			new String[] { (String)values.get(Base.KEY) }) == -1) {
			            				// TODO error occurred
			            				Log.e(TAG, "Could not update entity");
			            			} else {
			            				changed = true;
			            			}
		        	        	}
	        	        	}
	        	        }
	        	        cur.close();
        			}
        		}
        	}
        	
        	// TODO only send update notification if transaction actually succeeds.
        	if (changed) {
        		// TODO precalculate all of these URIs, which will save significant time on insertion
        		Uri changedUri = Uri.withAppendedPath(Store.Base.CONTENT_URI_BASE, app + "/" + kind + "/" + (String)values.get(Base.KEY));
        		mContext.getContentResolver().notifyChange(changedUri, null);
        		
        		// Also notify all join URIs.
        		Set<String> tables = getTableColumnMap(db).keySet();
        		for (Iterator<String> tableIter = tables.iterator(); tableIter.hasNext() ; ) {
        			String table = tableIter.next();
        			if (table.equals(kind)) {
        				continue;
        			}
        			changedUri = Uri.withAppendedPath(Store.Base.CONTENT_URI_BASE, app + "/" + kind + "." + table + "/" + (String)values.get(Base.KEY));
            		mContext.getContentResolver().notifyChange(changedUri, null);
            		changedUri = Uri.withAppendedPath(Store.Base.CONTENT_URI_BASE, app + "/" + table + "." + kind + "/");
            		mContext.getContentResolver().notifyChange(changedUri, null);
        		}
        	}
        	
        	return changed;
        }
        
        @Override
        public void onUpgrade(SQLiteDatabase db, int oldVersion, int newVersion) {
            Log.w(TAG, "Upgrading database from version " + oldVersion + " to "
                    + newVersion + ", which will destroy all old data");
            // TODO Check for any uncommitted dirty changes here before proceeding.
            for (String table : getTableColumnMap(db).keySet()) {
                db.execSQL("DROP TABLE IF EXISTS \"" + table + "\"");
            }
            mTableColumnMap.clear();
            populateTableColumnMap(db);
            onCreate(db);
        }
    }
    
    public void updateAllFromJson(String app, String kind, JsonParser jp, int count, Uri updateUri) {
    	// Get the database and run the query
        SQLiteDatabase db = getOpenHelper(app).getWritableDatabase();
    	getOpenHelper(app).insertAllFromJson(db, app, kind, jp, count, updateUri, Mode.UPSERT);
    }

    public class UploadData {
    	UploadData() {
    		count = 0;
    		data = "";
    		error = false;
    	}
    	int count;
    	String data;
    	boolean error;
    }
    
    public UploadData startUploadTransactionForKind(String app, String kind, Uri uri) {
    	UploadData response = new UploadData();
    	response.data = "[";
    	
        SQLiteDatabase db = getOpenHelper(app).getWritableDatabase();
        db.beginTransaction();
    	
    	Cursor c = query(uri, new String[] { Base.KEY, Base.CHANGES }, Base.DIRTY + " = 1", null, null);
    	c.moveToFirst();
    	int changesCol = c.getColumnIndex(Base.CHANGES);
    	int keyCol = c.getColumnIndex(Base.KEY);
    	boolean first = true;
    	while (!c.isAfterLast()) {
    		if (first) {
    			first = false;
    		} else {
    			response.data += ",\n";
    		}
    		String dataWithKey = null;
    		String key = c.getString(keyCol);
    		String data = c.getString(changesCol);
    		
			try {
				@SuppressWarnings("unchecked")
				Map<String, Object> values = mObjectMapper.readValue(data, Map.class);
	    		values.put(Base.KEY, key);
	    		dataWithKey = mObjectMapper.writeValueAsString(values);
			} catch (JsonParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (JsonMappingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
    		if (dataWithKey == null) {
    			Log.e(TAG, "Could not upload data for row: " + uri + ", " + key);
    			response.error = true;
    			dataWithKey = "{}";
    		}
    		response.data += dataWithKey;
    		response.count++;
    			
    		c.moveToNext();
    	}
    	response.data += "]\n";
    	c.close();
    	return response;
    }
    
    public void finishUploadTransactionForKind(String app, String kind, Uri uri, UploadData upload, boolean error) {
        SQLiteDatabase db = getOpenHelper(app).getWritableDatabase();
        
        try {
        	if (!error && upload.count > 0) {
        		String kindQuoted = "\"" + kind + "\"";
        		ContentValues values = new ContentValues();
        		values.put(Base.DIRTY, 0);
        		values.putNull(Base.CHANGES);
        		String where = Base.DIRTY + " = 1";
        		String queryWhere = extractWhereFromUri(uri);
        		if (queryWhere.length() > 0) {
        			where = "(" + queryWhere + ") AND " + where;
        		}
        		int count = db.update(kindQuoted, values, where, null);
        		if (upload.count != count) {
        			Log.e(TAG, "Error: number of entities changed before / after update: " + kind + 
        					", before=" + upload.count + ", after=" + count);
        			error = true;
        		}
        	}
        	if (!error) {
        		db.setTransactionSuccessful();
        	}
        } finally {
        	db.endTransaction();
        }
    }
    
    private MetaDatabaseHelper mMetaOpenHelper;
    private Map<String, DatabaseHelper> mOpenHelperMap;

	private ObjectMapper mObjectMapper;
    
	class Progress {
		boolean syncActive = false;
		int progressPercent = 0;
		String status = "";
	}
	Map<String, Progress> mActiveSyncMap;
	
	Progress getActiveSyncProgress(String key) {
		if (mActiveSyncMap.get(key) == null) {
			mActiveSyncMap.put(key, new Progress());
		}
		return mActiveSyncMap.get(key);
	}
    
	DatabaseHelper getOpenHelper(String app) {
		DatabaseHelper helper = mOpenHelperMap.get(app);
		if (helper == null) {
			Log.i(TAG, "Creating database helper for : " + app);
			if (!DatabaseHelper.isValidAppString(app)) {
				throw new IllegalArgumentException("Invalid app name : " + app);
			}
			helper = new DatabaseHelper(getContext(), this, app);
			mOpenHelperMap.put(app, helper);
		}
		return helper;
	}
	
	@Override
	public boolean onCreate() {
        mMetaOpenHelper = new MetaDatabaseHelper(getContext(), this);
        mOpenHelperMap = new HashMap<String, DatabaseHelper>();
        mActiveSyncMap = new HashMap<String,Progress>();
        mObjectMapper = new ObjectMapper();
        return true;
	}

	public Cursor metaTableQuery(int type, Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		final String pathQuery = getPathQuery(uri);
		
		SQLiteDatabase db = mMetaOpenHelper.getReadableDatabase();
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(Meta_Table.TABLE_NAME);
        if (type == ITEM_TABLE) {
        	qb.appendWhere(Meta_Table.PATH_QUERY + " = '" + pathQuery + "'");
        } else if (type == ITEM_TABLE_LIST) {
        	qb.appendWhere(Meta_Table.PATH_QUERY + " LIKE '" + pathQuery + "%'");
        }
        
        if (sortOrder == null || sortOrder.length() == 0) {
        	sortOrder = Meta_Table.DEFAULT_SORT_ORDER;
        }
		Cursor c = qb.query(db, Meta_Table.SQL_COLUMNS, selection, selectionArgs, null, null, sortOrder);
		Cursor newCursor = progressCursorFromMetaTable(c);
		c.close();
		return newCursor;
	}
	
	public Cursor metaMappingQuery(int type, Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		SQLiteDatabase db = mMetaOpenHelper.getReadableDatabase();
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        qb.setTables(Meta_Mapping.TABLE_NAME);
        if (type == ITEM_MAPPING) {
        	List<String> path = uri.getPathSegments();
    		String app = null;
    		app = path.get(1);
        	qb.appendWhere(Meta_Mapping.APP + " = '" + app + "'");
        }
        
        if (sortOrder == null || sortOrder.length() == 0) {
        	sortOrder = Meta_Mapping.DEFAULT_SORT_ORDER;
        }
		Cursor c = qb.query(db, projection, selection, selectionArgs, null, null, sortOrder);
		
		return c;
	}
	
	public MatrixCursor progressCursorFromMetaTable(Cursor old) {
		old.moveToFirst();
		int length = old.getCount();
		MatrixCursor newCursor = new MatrixCursor(Meta_Table.ALL_COLUMNS, length);
		while (!old.isAfterLast()) {
			// TODO increase column selection safety / hygiene here
			Progress p = getActiveSyncProgress(old.getString(1));
			String values[] = { old.getString(0), old.getString(1), old.getString(2), 
					p.syncActive ? "1" : "0", String.valueOf(p.progressPercent), p.status };
			newCursor.addRow(values);
			old.moveToNext();
		}
		newCursor.moveToPosition(-1);
		return newCursor;
	}
	
	static public String extractWhereFromUri(Uri uri) {
		String where = "";
		if (uri.getEncodedQuery() != null && uri.getEncodedQuery().length() > 0) {
			Map<String, String> query = Util.getQueryComponents(uri);
			for (Entry<String, String> s : query.entrySet()) {
				if (where.length() > 0) {
					where += " AND ";
				}
				where += s.getKey() + "= \"" + s.getValue() + "\"";
			}
		}
		return where;
	}
	
	static public void extractWhereFromUri(SQLiteQueryBuilder qb, Uri uri) {
		String where = extractWhereFromUri(uri);
		if (where.length() > 0) {
			qb.appendWhere(where);
		}
	}
	
	@Override
	public Cursor query(Uri uri, String[] projection, String selection,
			String[] selectionArgs, String sortOrder) {
		int uriType = sUriMatcher.match(uri);
		if (uriType == ITEM_TABLE || uriType == ITEM_TABLE_LIST) {
			return metaTableQuery(uriType, uri, projection, selection, selectionArgs, sortOrder);
		}
		if (uriType == ITEM_MAPPING || uriType == ITEM_MAPPING_LIST) {
			return metaMappingQuery(uriType, uri, projection, selection, selectionArgs, sortOrder);
		}
		
		List<String> path = uri.getPathSegments();
		// TODO validate path
		if (path.size() < 2) {
			 throw new IllegalArgumentException("Unknown URI " + uri);
		}
		
		String app = path.get(0);
		String kind = path.get(1);
		String key = null;
		String joinKind = null;
		if (path.size() >= 3) {
			key = path.get(2);
		}
		if (path.size() >= 4) {
			joinKind = path.get(3);
		}
		if (kind.contains(".")) {
			String kindParts[] = kind.split("\\.");
			if (kindParts.length == 2) {
				kind = kindParts[0];
				joinKind = kindParts[1];
			}
			
		}
		String quotedKind = "'" + kind + "'";
		
        SQLiteDatabase writableDb = getOpenHelper(app).getWritableDatabase();
        SQLiteQueryBuilder qb = new SQLiteQueryBuilder();
        // Check tables for existence
        if (writableDb != null) {
        	List<String> columns = getOpenHelper(app).getOrCreateKindTable(writableDb, app, kind);
        	List<String> joinColumns = null;
        	if (joinKind != null) {
            	joinColumns = getOpenHelper(app).getOrCreateKindTable(writableDb, app, joinKind);
        	}
        	if (projection != null) {
		        for (int i = 0; i < projection.length; ++i) {
		        	if (!projection[i].contains(".") && !projection[i].contains("(") &&
		        		(!columns.contains(projection[i]) && (joinColumns == null || !joinColumns.contains(projection[i])))) {
		        		projection[i] = "null as " + projection[i];
		        	}
		        }
	        }
        }
        
        // TODO use app also
        qb.setTables(quotedKind);

        // Extract any select arguments from the query param.
		extractWhereFromUri(qb, uri);
        
        switch (sUriMatcher.match(uri)) {
        case ITEM_LIST:
        	if (joinKind != null) {
        		String joinColumn = getOpenHelper(app).findJoinColumn(writableDb, app, kind, joinKind);
        		if (joinColumn == null) {
        			// TODO This code makes things more strict, but it causes initial opening to fail.
        			// if (selection == null) {
        			//	throw new IllegalArgumentException("Unknown Join URI " + uri);
        			// } else 
        			{
        				// Assume the selection will make the join reasonable.
        				qb.setTables("'" + kind + "', '" + joinKind + "'");
        			}
        		} else {
        			qb.setTables("'" + kind + "' join '" + joinKind 
        					+ "' on ('" + kind + "'." + joinColumn + " = '" + joinKind + "'." + joinColumn + ")");
        		}
        	} 
        	break;
        	
        case ITEM_KEY:
            qb.appendWhere(quotedKind + "." + Base.KEY + "= \"" + key + "\"");
            break;

        default:
            throw new IllegalArgumentException("Unknown URI " + uri);
        }

        // If no sort order is specified use the default
        String orderBy;
        if (TextUtils.isEmpty(sortOrder)) {
            orderBy = Base.DEFAULT_SORT_ORDER;
        } else {
            orderBy = sortOrder;
        }
        if (joinKind != null && orderBy.equals(Base.DEFAULT_SORT_ORDER)) {
        	orderBy = quotedKind + "." + orderBy;
        }
        
        // Get the database and run the query
        SQLiteDatabase db = getOpenHelper(app).getReadableDatabase();
        if (projection == null) {
        	projection = new String[] { "*", quotedKind + ".rowid as _id"};
        } else {
        	String[] newProjection = new String[projection.length + 1];
        	for (int i = 0; i < projection.length; ++i) {
        		newProjection[i] = projection[i];
        		if (projection[i].equals(Base.KEY)) {
        			newProjection[i] = quotedKind + "." + Base.KEY + " as " + Base.KEY;
        		}
        	}
        	newProjection[projection.length] = quotedKind + ".rowid as _id";
        	projection = newProjection;
        }
        // Poor design of this abstraction makes group by impossible. As a polite hack, split it out of the selection.
        String groupBy = null;
        if (selection != null) {
	        int index = Math.max(selection.indexOf("GROUP BY"), selection.indexOf("group by"));
	        if (index >= 0) {
	        	groupBy = selection.substring(index + 8, selection.length());
	        	selection = selection.substring(0, index);
	        }
        }
        
        Cursor c = qb.query(db, projection, selection, selectionArgs, groupBy, null, orderBy);
        
        // Tell the cursor what uri to watch, so it knows when its source data changes
        c.setNotificationUri(getContext().getContentResolver(), uri);
        return c;
	}

	@Override
	public String getType(Uri uri) {
		switch (sUriMatcher.match(uri)) {
			case ITEM_LIST:
			{
				List<String> path = uri.getPathSegments();
				return Store.Base.CONTENT_TYPE_BASE + path.get(0) + "." + path.get(1);
			}
			
			case ITEM_KEY:
			{
				List<String> path = uri.getPathSegments();
				return Store.Base.CONTENT_ITEM_TYPE_BASE + path.get(0) + "." + path.get(1);
			}
			
			case ITEM_TABLE_LIST:
			{
				return Store.Meta_Table.CONTENT_TYPE;
			}
			
			case ITEM_TABLE:
			{
				return Store.Meta_Table.CONTENT_ITEM_TYPE;
			}
			
			case ITEM_MAPPING_LIST:
			{
				return Store.Meta_Mapping.CONTENT_TYPE;
			}
			
			case ITEM_MAPPING:
			{
				return Store.Meta_Mapping.CONTENT_ITEM_TYPE;
			}
			
			default:
				throw new IllegalArgumentException("Unknown URI " + uri);
		}
	}

	public Uri metaTableInsert(Uri uri, ContentValues values) {
		String pathQuery = getPathQuery(uri);
		
		if (values != null && values.size() > 0) {
            throw new IllegalArgumentException("Illegal values to URI " + uri);
		}
		
		ContentValues initialValues = new ContentValues();
		initialValues.put(Meta_Table.PATH_QUERY, pathQuery);
		
		SQLiteDatabase db = mMetaOpenHelper.getWritableDatabase();
		if (db.insertWithOnConflict(Meta_Table.TABLE_NAME, null, initialValues, SQLiteDatabase.CONFLICT_IGNORE) == -1) {
			// TODO error occurred
			Log.e(TAG, "Could not insert meta entity: " + pathQuery);
		}
		
        getContext().getContentResolver().notifyChange(uri, null);
		return uri;
	}
	
	public Uri metaMappingInsert(Uri uri, ContentValues values) {
		List<String> path = uri.getPathSegments();
		String app = path.get(1);
		
		ContentValues initialValues = new ContentValues();
		initialValues.putAll(values);
		initialValues.put(Meta_Mapping.APP, app);
		
		SQLiteDatabase db = mMetaOpenHelper.getWritableDatabase();
		if (db.insertWithOnConflict(Meta_Mapping.TABLE_NAME, null, initialValues, SQLiteDatabase.CONFLICT_IGNORE) == -1) {
			// TODO error occurred
			Log.e(TAG, "Could not insert meta entity: " + app);
		}
		
        getContext().getContentResolver().notifyChange(uri, null);
		return uri;
	}
	
	@Override
	public Uri insert(Uri uri, ContentValues values) {
		int uriType = sUriMatcher.match(uri);
		
		switch (uriType) {
		case ITEM_TABLE: 
			return metaTableInsert(uri, values);
		case ITEM_MAPPING:
			return metaMappingInsert(uri, values);	
		}
		
		return null;
	}

	@Override
	public int delete(Uri uri, String selection, String[] selectionArgs) {
		throw new SQLException("Delete is not implemented for this ContentProvider");
	}

	static String getPathQuery(Uri uri) {
		String pathQuery = null;
		String path = uri.getEncodedPath();
		if (path.indexOf('/', 1) == -1) {
			pathQuery = "/";
		} else {
			pathQuery = path.substring(path.indexOf('/', 1));
		}
		if (uri.getEncodedQuery() != null) {
			pathQuery += "?" + uri.getEncodedQuery();
		}
		return pathQuery;
	}
	
	public int metaTableUpdate(Uri uri, ContentValues values, String where, String[] whereArgs) {
		if (where != null && where.length() > 0) {
            throw new IllegalArgumentException("Illegal selection to URI " + uri);
		}
		
		String pathQuery = getPathQuery(uri);
		
		if (values.containsKey(Meta_Table.SYNC_ACTIVE)) {
			getActiveSyncProgress(pathQuery).syncActive = values.getAsBoolean(Meta_Table.SYNC_ACTIVE);
			values.remove(Meta_Table.SYNC_ACTIVE);
		}
		if (values.containsKey(Meta_Table.PROGRESS_PERCENT)) {
			getActiveSyncProgress(pathQuery).progressPercent = values.getAsInteger(Meta_Table.PROGRESS_PERCENT);
			values.remove(Meta_Table.PROGRESS_PERCENT);
		}
		if (values.containsKey(Meta_Table.STATUS)) {
			getActiveSyncProgress(pathQuery).status = values.getAsString(Meta_Table.STATUS);
			values.remove(Meta_Table.STATUS);
		}
		
		int count = 1;
		if (values.size() > 0) {
			SQLiteDatabase db = mMetaOpenHelper.getWritableDatabase();
			count = db.update(Meta_Table.TABLE_NAME, values, Meta_Table.PATH_QUERY + "= \"" + pathQuery + "\"", null);
		}
  
		getContext().getContentResolver().notifyChange(uri, null);
		return count;
	}
	
	public int metaMappingUpdate(Uri uri, ContentValues values, String where, String[] whereArgs) {
		if (where != null && where.length() > 0) {
            throw new IllegalArgumentException("Illegal selection to URI " + uri);
		}
		
		List<String> path = uri.getPathSegments();
		String app = path.get(1);
		
		SQLiteDatabase db = mMetaOpenHelper.getWritableDatabase();
		int count = db.update(Meta_Mapping.TABLE_NAME, values, Meta_Mapping.APP + "= \"" + app + "\"", null);
  
		getContext().getContentResolver().notifyChange(uri, null);
		return count;
	}
	
	@SuppressWarnings("unchecked")
	@Override
	public int update(Uri uri, ContentValues values, String where,
			String[] whereArgs) {
		int uriType = sUriMatcher.match(uri);
		
		switch (uriType) {
		case ITEM_TABLE: 
			return metaTableUpdate(uri, values, where, whereArgs);
		case ITEM_MAPPING:
			return metaMappingUpdate(uri, values, where, whereArgs);
		case ITEM_KEY:
		case ITEM_LIST:
			// Do nothing
			break;
		default:
            throw new IllegalArgumentException("Unknown URI " + uri);
		}
		
		// Do nothing if there are no changes to be made.
		if (values == null || values.size() == 0) {
			return 0;
		}
		
		List<String> path = uri.getPathSegments();
		// TODO validate path
		String app = path.get(0);
		String kind = path.get(1);
		String kindQuoted = "\"" + kind + "\"";
		String key = null;
		if (path.size() > 2) {
			key = path.get(2);
		}
		
        SQLiteDatabase db = getOpenHelper(app).getWritableDatabase();
        // Check tables for existence
        if (values != null) {
        	Iterator<Entry<String, Object>> iter = values.valueSet().iterator();
	        while (iter.hasNext()) {
	        	getOpenHelper(app).createColumn(db, app, kind, iter.next().getKey());
	        }
        }
        
        // TODO Plan for update:
        // Produce JSON version of values
        // Start TX
        // Query all affected entities
        // If any are __dirty__ and have __changes__, merge changes
        // Apply each change (to individual entities, and/or to the whole list)
        // For the whole list, assume we apply to all assuming not-dirty, and then 
        //              fix the dirty ones (this does not scale)
        // Or, for changing multiple entities, simply do one at a time:
        // for entities with where AND __dirty__ == true : apply merge
        // for entities with where and __dirty__ == false: just add data

        String whereSuffix = " ";
        if (uriType == ITEM_KEY) {
        	whereSuffix += "AND (" + Base.KEY + " = \"" + key + "\") ";
        }
        whereSuffix += (!TextUtils.isEmpty(where) ? "AND (" + where + ") " : "");
        
        String queryWhere = extractWhereFromUri(uri);
        whereSuffix += (!TextUtils.isEmpty(queryWhere) ? "AND (" + queryWhere + ") " : "");
        
        String changed = null;
        HashMap<String, Object> changes = new HashMap<String, Object>();
        for (Entry<String, Object> e : values.valueSet()) {
        	changes.put(e.getKey(), e.getValue());
        }
        
        try {
			changed = mObjectMapper.writeValueAsString(changes);
		} catch (JsonGenerationException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (JsonMappingException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		if (changed == null) {
            throw new IllegalArgumentException("Could not serialize data for update: " + values.toString());
		}
		
        values.put(Base.DIRTY, 1);
        
        int count = 0;
        db.beginTransaction();
        try {
        
        	Cursor c = db.query(kindQuoted, new String[] { Base.KEY, Base.CHANGES }, 
        			"(" + Base.DIRTY + " = 1)" + whereSuffix, whereArgs, null, null, null);
        	
        	c.moveToFirst();
        	while (!c.isAfterLast()) {
        		String rowKey = c.getString(0);
        		String rowChanged = c.getString(1);
        		
        		Map<String, Object> rowChanges = null;
        		try {
					rowChanges = mObjectMapper.readValue(rowChanged, Map.class);
				} catch (JsonParseException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (JsonMappingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				if (rowChanges == null) {
		            throw new IllegalArgumentException("Could not parse existing changes for row " + uri + ", " + 
		            		rowKey + ": " + rowChanged);
				}
				rowChanges.putAll(changes);
				try {
					rowChanged = mObjectMapper.writeValueAsString(rowChanges);
				} catch (JsonGenerationException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (JsonMappingException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				} catch (IOException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				if (changed == null) {
		            throw new IllegalArgumentException("Could not serialize data for update: " + rowChanges.toString());
				}
				values.put(Base.CHANGES, rowChanged);

	        	count += db.update(kindQuoted, values, "(" + Base.KEY + " = \"" + rowKey + "\")" + whereSuffix, whereArgs);
	        	c.moveToNext();
        	}
        	c.close();
        	
        	values.put(Base.CHANGES, changed);
        	count += db.update(kindQuoted, values, "(" + Base.DIRTY + " = 0)" + whereSuffix, whereArgs);
        	
        	db.setTransactionSuccessful();
        } finally {
        	db.endTransaction();
        }

        getContext().getContentResolver().notifyChange(uri, null);
        return count;
	}

	
	static {
        sUriMatcher = new UriMatcher(UriMatcher.NO_MATCH);
        sUriMatcher.addURI(Store.AUTHORITY, Meta_Table.TABLE_NAME + "/*/*", ITEM_TABLE);
        sUriMatcher.addURI(Store.AUTHORITY, Meta_Table.TABLE_NAME + "/*", ITEM_TABLE_LIST);
        sUriMatcher.addURI(Store.AUTHORITY, Meta_Table.TABLE_NAME + "/", ITEM_TABLE_LIST);
        sUriMatcher.addURI(Store.AUTHORITY, Meta_Mapping.TABLE_NAME + "/*", ITEM_MAPPING);
        sUriMatcher.addURI(Store.AUTHORITY, Meta_Mapping.TABLE_NAME + "/", ITEM_MAPPING_LIST);
        sUriMatcher.addURI(Store.AUTHORITY, "*/*", ITEM_LIST);
        sUriMatcher.addURI(Store.AUTHORITY, "*/*/*", ITEM_KEY);
	}
}
