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

// Note: This code is based loosely on an example provided by Nick Johnson at
// http://blog.notdot.net/2010/05/Authenticating-against-App-Engine-from-an-Android-app

package org.mantasync;

import java.io.IOException;

import org.apache.http.HttpResponse;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.params.ClientPNames;
import org.apache.http.cookie.Cookie;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.params.HttpParams;
import org.apache.http.params.HttpProtocolParams;

import android.accounts.Account;
import android.accounts.AccountManager;
import android.accounts.AccountManagerCallback;
import android.accounts.AccountManagerFuture;
import android.accounts.AuthenticatorException;
import android.accounts.OperationCanceledException;
import android.content.Context;
import android.content.Intent;
import android.content.pm.PackageInfo;
import android.content.pm.PackageManager.NameNotFoundException;
import android.net.Uri;
import android.os.AsyncTask;
import android.os.Build;
import android.os.Bundle;
import android.util.Log;

public class AuthHttpRequest {
	static final String TAG = "AuthHttpRequest";

	public static final int RESULT_RETRY = 0;
	public static final int RESULT_SUCCESS = 1;
	public static final int RESULT_FAILURE = 2;
	
	private DefaultHttpClient mHttpClient = new DefaultHttpClient();
	private Context mContext;
	private Account mAccount;
	private String mServerUrl;
	
	public Account getAccount() {
		return mAccount;
	}

	public HttpClient getHttpClient() {
		return mHttpClient;
	}
	
	public String getServerUrl() {
		return mServerUrl;
	}
	
	AuthHttpRequest(Context context, Account account, String serverUrl) {
		mContext = context;
		mAccount = account;
		mServerUrl = serverUrl;
		
		String app_name = "unknown";
		String app_ver = "unknown";
		try
		{
			PackageInfo info = context.getPackageManager().getPackageInfo(context.getPackageName(), 0);
			app_name = info.packageName;
		    app_ver = info.versionName;
		}
		catch (NameNotFoundException e)
		{
		    Log.v(TAG, e.getMessage());
		}
		String userAgent = "Android/" + Build.VERSION.RELEASE + " (" + Build.MODEL + ", " + app_name + ", " + app_ver + ")";
		
		HttpParams params = mHttpClient.getParams();
		HttpProtocolParams.setUserAgent(params, userAgent);
	}
	
	class PopulateAuthCallback {
		public void onPopulateDone(int result) {
			
		}
	}
	
	public void PopulateHttpClientWithAuthToken(PopulateAuthCallback callback) {
		AccountManager accountManager = AccountManager.get(mContext.getApplicationContext());
		accountManager.getAuthToken(mAccount, "ah", false, new GetAuthTokenCallback(callback), null);
	}

	private class GetAuthTokenCallback implements AccountManagerCallback<Bundle> {
		PopulateAuthCallback mCallback;
		
		GetAuthTokenCallback(PopulateAuthCallback callback) {
			mCallback = callback;
		}
		
		public void run(AccountManagerFuture<Bundle> result) {
			Bundle bundle;
			try {
				bundle = result.getResult();
				Log.e(TAG, "At GetAuthTokenCallback: " + bundle.toString());
				Intent intent = (Intent)bundle.get(AccountManager.KEY_INTENT);
				if(intent != null) {
					// User input required
					intent.addFlags(Intent.FLAG_ACTIVITY_NEW_TASK);
					mContext.startActivity(intent);
					if (mCallback != null) {
						mCallback.onPopulateDone(RESULT_RETRY);
					}
				} else {
					onGetAuthToken(bundle, mCallback);
				}
			} catch (OperationCanceledException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (AuthenticatorException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	};

	protected void onGetAuthToken(Bundle bundle, PopulateAuthCallback callback) {
		String auth_token = bundle.getString(AccountManager.KEY_AUTHTOKEN);
		Log.e(TAG, "At onGetAuthToken: " + auth_token);
		new GetCookieTask(callback).execute(auth_token);
	}

	private class GetCookieTask extends AsyncTask<String, Void, Integer> {
		PopulateAuthCallback mCallback;
		
		GetCookieTask(PopulateAuthCallback callback) {
			mCallback = callback;
		}
		
		protected Integer doInBackground(String... tokens) {
			try {
				// Don't follow redirects
				mHttpClient.getParams().setBooleanParameter(ClientPNames.HANDLE_REDIRECTS, false);
				
				String hostOnly = Uri.parse(mServerUrl).buildUpon().path("/").query("").build().toString();
				String url = hostOnly + "_ah/login?continue=" + hostOnly + "_ah/unreachable&auth=" + tokens[0];
				HttpGet http_get = new HttpGet(url);
				HttpResponse response = mHttpClient.execute(http_get);
				response.getEntity().getContent().close();
				if(response.getStatusLine().getStatusCode() != 302) {
					// Response should be a redirect
					Log.e(TAG, "Error fetching auth URL. (Response code " + response.getStatusLine().getStatusCode() + ") " + 
							"Invalidating auth token.");
					AccountManager accountManager = AccountManager.get(mContext.getApplicationContext());
					accountManager.invalidateAuthToken(mAccount.type, tokens[0]);
					
					return RESULT_FAILURE;
				}

				Log.e(TAG, "At GetCookieTask Redirect URL: " + response.getHeaders("Location")[0].getValue());
				for(Cookie cookie : mHttpClient.getCookieStore().getCookies()) {
					if(cookie.getName().equals("ACSID")) {
						Log.e(TAG, "At GetCookieTask ACSID: " + cookie.getValue());
						return RESULT_SUCCESS;
					}
					if(cookie.getName().equals("SACSID")) {
						Log.e(TAG, "At GetCookieTask SACSID: " + cookie.getValue());
						return RESULT_SUCCESS;
					}
				}
				
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} finally {
				mHttpClient.getParams().setBooleanParameter(ClientPNames.HANDLE_REDIRECTS, true);
			}
			return RESULT_FAILURE;
		}
		
		protected void onPostExecute(Integer result) {
			if (mCallback != null) {
				mCallback.onPopulateDone(result);
			}
		}
	}
}
