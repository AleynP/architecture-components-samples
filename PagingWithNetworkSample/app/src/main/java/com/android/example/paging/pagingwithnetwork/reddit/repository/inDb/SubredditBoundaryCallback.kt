/*
 * Copyright (C) 2017 The Android Open Source Project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.android.example.paging.pagingwithnetwork.reddit.repository.inDb

import androidx.annotation.MainThread
import androidx.paging.PagedList
import androidx.paging.PagingRequestHelper
import com.android.example.paging.pagingwithnetwork.reddit.api.RedditApi
import com.android.example.paging.pagingwithnetwork.reddit.vo.RedditPost
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import java.util.concurrent.Executor

/**
 * This boundary callback gets notified when user reaches to the edges of the list such that the
 * database cannot provide any more data.
 * <p>
 * The boundary callback might be called multiple times for the same direction so it does its own
 * rate limiting using the PagingRequestHelper class.
 */
class SubredditBoundaryCallback(
        private val subredditName: String,
        private val webservice: RedditApi,
        private val handleResponse: (String, RedditApi.ListingResponse?) -> Unit,
        private val ioExecutor: Executor,
        private val networkPageSize: Int)
    : PagedList.BoundaryCallback<RedditPost>() {

    private val helper = PagingRequestHelper(ioExecutor)

    /**
     * Database returned 0 items. We should query the backend for more items.
     */
    @MainThread
    override fun onZeroItemsLoaded() {
        helper.runIfNotRunning(PagingRequestHelper.RequestType.INITIAL) {
            fetchNetworkAndWriteToDb(it) {
                webservice.getTop(subreddit = subredditName, limit = networkPageSize)
            }
        }
    }

    /**
     * User reached to the end of the list.
     */
    @MainThread
    override fun onItemAtEndLoaded(itemAtEnd: RedditPost) {
        helper.runIfNotRunning(PagingRequestHelper.RequestType.AFTER) {
            fetchNetworkAndWriteToDb(it) {
                webservice.getTopAfter(subredditName, itemAtEnd.name, networkPageSize)
            }
        }
    }

    /**
     * every time it gets new items, boundary callback simply inserts them into the database and
     * paging library takes care of refreshing the list if necessary.
     */
    private fun insertItemsIntoDb(
            response: RedditApi.ListingResponse,
            it: PagingRequestHelper.Request.Callback
    ) {
        ioExecutor.execute {
            handleResponse(subredditName, response)
            it.recordSuccess()
        }
    }

    override fun onItemAtFrontLoaded(itemAtFront: RedditPost) {
        // ignored, since we only ever append to what's in the DB
    }

    private fun fetchNetworkAndWriteToDb(
            it: PagingRequestHelper.Request.Callback,
            block: suspend () -> RedditApi.ListingResponse
    ) {
        // TODO: Not GlobalScope
        GlobalScope.launch(ioExecutor.asCoroutineDispatcher()) {
            try {
                insertItemsIntoDb(block(), it)
            } catch (e: Throwable) {
                it.recordFailure(e)
            }
        }
    }
}