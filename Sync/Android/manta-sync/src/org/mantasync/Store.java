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

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import android.net.Uri;
import android.provider.BaseColumns;

/**
 * Convenience definitions for StoreProvider
 */
public final class Store {
    public static final String AUTHORITY = "org.mantasync.Store";

    public static final String APPLICATION_NAME = "Sync Utility";
    
	public static final SimpleDateFormat sDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
	
	static {
		sDateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
	}
	
    // This class cannot be instantiated
    private Store() {}

    /**
     * Base table definition
     */
    public static final class Base implements BaseColumns {
        // This class cannot be instantiated
        private Base() { }
        
	    /**
	     * The content:// style URL for this table
	     */
	    public static final Uri CONTENT_URI_BASE = Uri.parse("content://" + AUTHORITY + "/");
	
	    /**
	     * The MIME type of {@link #CONTENT_URI} providing a directory of items.
	     */
	    public static final String CONTENT_TYPE_BASE = "vnd.android.cursor.dir/vnd.mantastore.";
	
	    /**
	     * The MIME type of a {@link #CONTENT_URI} sub-directory of a single item.
	     */
	    public static final String CONTENT_ITEM_TYPE_BASE = "vnd.android.cursor.item/vnd.mantastore.";
	
	    /**
	     * The default sort order for this table
	     */
	    public static final String DEFAULT_SORT_ORDER = "key ASC";
	    
	    /**
         * The type for the row.
         * <P>Type: TEXT</P>
         */
        public static final String TYPE = "type";
        
        /**
         * The key for the row.
         * <P>Type: TEXT</P>
         */
        public static final String KEY = "key";
        
        /**
         * The revision for the row, set by the server only.
         * <P>Type: TEXT</P>
         */
        public static final String REV = "rev";
        
        /**
         * The date modified for the row, set by the server only.
         * <P>Type: INTEGER</P>
         */
        public static final String DATE = "date";

        /**
         * The dirty bit for the row, set locally only.
         * <P>Type: BOOL</P>
         */
        public static final String DIRTY = "__dirty__";
        
        /**
         * The changes made to this row, set locally only. Stored as a JSON string.
         * <P>Type: TEXT</P>
         */
        public static final String CHANGES = "__changes__";
        
        /**
         * Built-in columns, not user data.
         * <P>Type: String[]</P>
         */
        public static final String[] BUILT_IN_COLUMNS = new String[] { 
        	TYPE, KEY, REV, DATE, DIRTY, CHANGES
        	};
        public static final List<String> BUILT_IN_COLUMNS_LIST = Arrays.asList(BUILT_IN_COLUMNS);

        /**
         * The base projection, for _ID access.
         * <P>Type: Map<String,String></P>
         */
        public static final Map<String, String> sProjection;
        
        static {
        	sProjection = new HashMap<String,String>();
        	sProjection.put(Base._ID, "ROWID");
        }
    }
    
    /**
     * Meta Synced Tables table definition
     */
    public static final class Meta_Table implements BaseColumns {
        // This class cannot be instantiated
        private Meta_Table() {}
        
	    /**
	     * The MIME type of {@link #CONTENT_URI} providing a directory of items.
	     */
	    public static final String TABLE_NAME = "SyncedTable";
        
	    /**
	     * The content:// style URL for this table
	     */
	    public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY + "/" + TABLE_NAME);
	
	    /**
	     * The MIME type of {@link #CONTENT_URI} providing a directory of items.
	     */
	    public static final String CONTENT_TYPE = "vnd.android.cursor.dir/vnd.mantastore_meta." + TABLE_NAME;
	
	    /**
	     * The MIME type of a {@link #CONTENT_URI} sub-directory of a single item.
	     */
	    public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.mantastore_meta." + TABLE_NAME;
	
	    /**
	     * The default sort order for this table
	     */
	    public static final String DEFAULT_SORT_ORDER = "rowid DESC";
        
        /**
         * The unique key for the row, the path+query component of the URI.
         * <P>Type: TEXT</P>
         */
        public static final String PATH_QUERY = "path_query";   
        
        /**
         * Time last synced, as an int. Will be 0 if never synced.
         * <P>Type: INT</P>
         */
        public static final String LAST_SYNCED = "last_synced";   

        /**
         * Is a sync currently active?
         * <P>Type: INT</P>
         */
        public static final String SYNC_ACTIVE = "sync_active";  
        
        /**
         * Progress percentage of the current sync. If the value is -1, show infinite / barbershop bar.
         * <P>Type: INT</P>
         */
        public static final String PROGRESS_PERCENT = "progress_percent"; 
        
        /**
         * Textual status of the sync.
         * <P>Type: TEXT</P>
         */
        public static final String STATUS = "status"; 

        /**
         * Columns contained in SQL directly, as opposed to in-memory.
         * <P>Type: String[]</P>
         */
        public static final String[] SQL_COLUMNS = new String[] { _ID, PATH_QUERY, LAST_SYNCED };

        /**
         * Columns contained in SQL directly, as opposed to in-memory.
         * <P>Type: String[]</P>
         */
        public static final String[] IN_MEMORY_COLUMNS = new String[] { SYNC_ACTIVE, PROGRESS_PERCENT, STATUS };

        /**
         * Columns contained in SQL directly, as opposed to in-memory.
         * <P>Type: String[]</P>
         */
        public static final String[] ALL_COLUMNS = new String[] { 
        	_ID, PATH_QUERY, LAST_SYNCED, SYNC_ACTIVE, PROGRESS_PERCENT, STATUS 
        	};
    }
    
    /**
     * Meta Synced Tables table definition
     */
    public static final class Meta_Mapping implements BaseColumns {
        // This class cannot be instantiated
        private Meta_Mapping() {}
        
	    /**
	     * The MIME type of {@link #CONTENT_URI} providing a directory of items.
	     */
	    public static final String TABLE_NAME = "ServerMapping";
        
	    /**
	     * The content:// style URL for this table
	     */
	    public static final Uri CONTENT_URI = Uri.parse("content://" + AUTHORITY + "/" + TABLE_NAME);
	
	    /**
	     * The MIME type of {@link #CONTENT_URI} providing a directory of items.
	     */
	    public static final String CONTENT_TYPE = "vnd.android.cursor.dir/vnd.mantastore_meta." + TABLE_NAME;
	
	    /**
	     * The MIME type of a {@link #CONTENT_URI} sub-directory of a single item.
	     */
	    public static final String CONTENT_ITEM_TYPE = "vnd.android.cursor.item/vnd.mantastore_meta." + TABLE_NAME;
	
	    /**
	     * The default sort order for this table
	     */
	    public static final String DEFAULT_SORT_ORDER = "rowid ASC";
        
        /**
         * The unique key for the row, the app symbolic name used in URIs and SyncedTable.
         * <P>Type: TEXT</P>
         */
        public static final String APP = "app";   
        
        /**
         * The app identifier this app should be mapped to when talking to a remote server. May be null.
         * <P>Type: TEXT</P>
         */
        public static final String MAPPED_APP = "mapped_app";   

        /**
         * The server URL to use for this app, as an override over the default server. May be null.
         * <P>Type: TEXT</P>
         */
        public static final String MAPPED_URL = "sync_active";  

        /**
         * Columns contained in SQL.
         * <P>Type: String[]</P>
         */
        public static final String[] ALL_COLUMNS = new String[] { _ID, APP, MAPPED_APP, MAPPED_URL };

    }
}
