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
import androidx.lifecycle.LiveData
import androidx.lifecycle.MutableLiveData
import androidx.paging.PagingConfig
import androidx.paging.PagingData
import androidx.paging.PagingDataFlow
import com.android.example.paging.pagingwithnetwork.reddit.api.RedditApi
import com.android.example.paging.pagingwithnetwork.reddit.db.RedditDb
import com.android.example.paging.pagingwithnetwork.reddit.repository.NetworkState
import com.android.example.paging.pagingwithnetwork.reddit.repository.RedditPostRepository
import com.android.example.paging.pagingwithnetwork.reddit.vo.RedditPost
import kotlinx.coroutines.GlobalScope
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.launch
import java.io.IOException
import java.util.concurrent.Executor

/**
 * Repository implementation that uses a database PagedList + a boundary callback to return a
 * listing that loads in pages.
 */
class DbRedditPostRepository(
        val db: RedditDb,
        private val redditApi: RedditApi,
        private val ioExecutor: Executor,
        private val networkPageSize: Int = DEFAULT_NETWORK_PAGE_SIZE
) : RedditPostRepository {

    /**
     * Inserts the response into the database while also assigning position indices to items.
     */
    private fun insertResultIntoDb(subredditName: String, body: RedditApi.ListingResponse?) {
        body!!.data.children.let { posts ->
            db.runInTransaction {
                val start = db.posts().getNextIndexInSubreddit(subredditName)
                val items = posts.mapIndexed { index, child ->
                    child.data.indexInResponse = start + index
                    child.data
                }
                db.posts().insert(items)
            }
        }
    }

    /**
     * When refresh is called, we simply run a fresh network request and when it arrives, clear
     * the database table and insert all new items in a transaction.
     * <p>
     * Since the PagedList already uses a database bound data source, it will automatically be
     * updated after the database transaction is finished.
     */
    @MainThread
    private fun refresh(subredditName: String): LiveData<NetworkState> {
        val networkState = MutableLiveData<NetworkState>()
        networkState.value = NetworkState.LOADING

        // TODO: not GlobalScope
        GlobalScope.launch(ioExecutor.asCoroutineDispatcher()) {
            try {
                val data = redditApi.getTop(subredditName, networkPageSize)
                db.runInTransaction {
                    db.posts().deleteBySubreddit(subredditName)
                    insertResultIntoDb(subredditName, data)
                }

                networkState.postValue(NetworkState.LOADED)
            } catch (e: IOException) {
                networkState.postValue(NetworkState.error(e.message))
            }
        }
        return networkState
    }

    /**
     * Returns a Listing for the given subreddit.
     */
    override fun postsOfSubreddit(subReddit: String, pageSize: Int): Flow<PagingData<RedditPost>> {
        // create a boundary callback which will observe when the user reaches to the edges of
        // the list and update the database with extra data.
        val boundaryCallback = SubredditBoundaryCallback(
                webservice = redditApi,
                subredditName = subReddit,
                handleResponse = this::insertResultIntoDb,
                ioExecutor = ioExecutor,
                networkPageSize = networkPageSize)

        return PagingDataFlow(
                config = PagingConfig(pageSize),
                pagingSourceFactory = db.posts().postsBySubreddit(subReddit).asPagingSourceFactory()
        )
    }

    companion object {
        private const val DEFAULT_NETWORK_PAGE_SIZE = 10
    }
}

