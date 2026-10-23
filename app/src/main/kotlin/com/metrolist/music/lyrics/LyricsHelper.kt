/**
 * Metrolist Project (C) 2026
 * Licensed under GPL-3.0 | See git history for contributors
 */

package com.metrolist.music.lyrics

import android.content.Context
import android.util.LruCache
import com.metrolist.music.constants.LyricsProviderOrderKey
import com.metrolist.music.db.entities.LyricsEntity.Companion.LYRICS_NOT_FOUND
import com.metrolist.music.models.MediaMetadata
import com.metrolist.music.utils.NetworkConnectivityObserver
import com.metrolist.music.utils.dataStore
import com.metrolist.music.utils.reportException
import dagger.hilt.android.qualifiers.ApplicationContext
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelChildren
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.flow.distinctUntilChanged
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.launch
import kotlinx.coroutines.withTimeoutOrNull
import timber.log.Timber
import javax.inject.Inject

private const val MAX_LYRICS_FETCH_MS = 25000L
private const val PER_PROVIDER_TIMEOUT_MS = 8000L
private const val PROVIDER_NONE = ""

class LyricsHelper
@Inject
constructor(
    @ApplicationContext private val context: Context,
    private val networkConnectivity: NetworkConnectivityObserver,
) {
    val preferred =
        context.dataStore.data
            .map { preferences ->
                resolveLyricsProviders(preferences)
            }.distinctUntilChanged()

    private val cache = LruCache<String, List<LyricsResult>>(MAX_CACHE_SIZE)
    private var currentLyricsJob: Job? = null

    suspend fun getLyrics(mediaMetadata: MediaMetadata): LyricsWithProvider {
        currentLyricsJob?.cancel()

        val cached = cache.get(mediaMetadata.id)?.firstOrNull()
        if (cached != null) {
            return LyricsWithProvider(cached.lyrics, cached.providerName)
        }

        val orderedProviders = context.dataStore.data
            .map { preferences -> resolveLyricsProviders(preferences) }
            .first()

        val isNetworkAvailable = try {
            networkConnectivity.isCurrentlyConnected()
        } catch (e: Exception) {
            true
        }

        if (!isNetworkAvailable) {
            return LyricsWithProvider(LYRICS_NOT_FOUND, PROVIDER_NONE)
        }

        val result = withTimeoutOrNull(MAX_LYRICS_FETCH_MS) {
            val primaryArtist = mediaMetadata.artists.firstOrNull()?.name
            val allArtists = mediaMetadata.artists.joinToString { it.name }
            val cleanedTitle = LyricsUtils.cleanTitleForSearch(mediaMetadata.title, primaryArtist)
            val enabledProviders = orderedProviders.filter { it.isEnabled(context) }

            Timber.tag("LyricsHelper").d("Starting parallel fetch for: $cleanedTitle by $allArtists")
            Timber.tag("LyricsHelper").d("Enabled providers in order: ${enabledProviders.joinToString { it.name }}")

            if (enabledProviders.isEmpty()) {
                return@withTimeoutOrNull LyricsWithProvider(LYRICS_NOT_FOUND, PROVIDER_NONE)
            }

            coroutineScope {
                val deferredList = enabledProviders.map { provider ->
                    provider to async(Dispatchers.IO) {
                        try {
                            withTimeoutOrNull(PER_PROVIDER_TIMEOUT_MS) {
                                provider.getLyrics(
                                    context,
                                    mediaMetadata.id,
                                    cleanedTitle,
                                    allArtists,
                                    mediaMetadata.duration,
                                    mediaMetadata.album?.title,
                                )
                            }
                        } catch (e: CancellationException) {
                            throw e
                        } catch (e: Exception) {
                            Timber.tag("LyricsHelper").w("${provider.name} threw: ${e.message}")
                            null
                        }
                    }
                }

                // Check priority #1 provider with a short grace window (up to 2000ms)
                val topProvider = deferredList.firstOrNull()
                if (topProvider != null) {
                    val topRes = try {
                        withTimeoutOrNull(2000L) { topProvider.second.await() }
                    } catch (e: Exception) {
                        null
                    }
                    if (topRes != null && topRes.isSuccess) {
                        val raw = topRes.getOrNull()
                        if (!raw.isNullOrBlank()) {
                            val filtered = LyricsUtils.filterLyricsCreditLines(raw)
                            if (lyricsTextLooksSynced(filtered) || topProvider.first.name == "BetterLyrics") {
                                Timber.tag("LyricsHelper").i("Got fast lyrics from top provider ${topProvider.first.name}")
                                coroutineContext.cancelChildren()
                                return@coroutineScope LyricsWithProvider(filtered, topProvider.first.name)
                            }
                        }
                    }
                }

                // Check other providers in preference order
                var fallbackResult: LyricsWithProvider? = null
                for ((provider, deferred) in deferredList) {
                    val providerResult = try {
                        if (deferred.isCompleted) {
                            deferred.getCompleted()
                        } else {
                            withTimeoutOrNull(2500L) { deferred.await() }
                        }
                    } catch (e: Exception) {
                        null
                    }

                    if (providerResult != null && providerResult.isSuccess) {
                        val raw = providerResult.getOrNull()
                        if (!raw.isNullOrBlank()) {
                            val filtered = LyricsUtils.filterLyricsCreditLines(raw)
                            val isSynced = lyricsTextLooksSynced(filtered)
                            if (isSynced || provider.name == "BetterLyrics") {
                                Timber.tag("LyricsHelper").i("Got synced lyrics from ${provider.name}")
                                coroutineContext.cancelChildren()
                                return@coroutineScope LyricsWithProvider(filtered, provider.name)
                            } else if (fallbackResult == null) {
                                fallbackResult = LyricsWithProvider(filtered, provider.name)
                            }
                        }
                    }
                }

                if (fallbackResult != null) {
                    Timber.tag("LyricsHelper").i("Using fallback lyrics from ${fallbackResult.provider}")
                    return@coroutineScope fallbackResult
                }

                Timber.tag("LyricsHelper").w("No lyrics found after parallel check")
                LyricsWithProvider(LYRICS_NOT_FOUND, PROVIDER_NONE)
            }
        }

        return result ?: LyricsWithProvider(LYRICS_NOT_FOUND, PROVIDER_NONE)
    }

    suspend fun getAllLyrics(
        mediaId: String,
        songTitle: String,
        songArtists: String,
        duration: Int,
        album: String? = null,
        callback: (LyricsResult) -> Unit,
    ) {
        currentLyricsJob?.cancel()

        val cacheKey = "$songArtists-$songTitle".replace(" ", "")
        cache.get(cacheKey)?.let { results ->
            results.forEach { callback(it) }
            return
        }

        val isNetworkAvailable = try {
            networkConnectivity.isCurrentlyConnected()
        } catch (e: Exception) {
            true
        }

        if (!isNetworkAvailable) return

        val allResult = mutableListOf<LyricsResult>()
        currentLyricsJob = CoroutineScope(SupervisorJob()).launch {
            val cleanedTitle = LyricsUtils.cleanTitleForSearch(songTitle, songArtists)
            val allProviders = context.dataStore.data
                .map { preferences -> resolveLyricsProviders(preferences) }
                .first()
            val enabledProviders = allProviders.filter { it.isEnabled(context) }

            val otherProviders = enabledProviders.filter { it.name != "LyricsPlus" }
            val lyricsPlusProvider = enabledProviders.find { it.name == "LyricsPlus" }

            val callbackMutex = Any()

            val otherJobs = otherProviders.map { provider ->
                launch {
                    try {
                        provider.getAllLyrics(context, mediaId, cleanedTitle, songArtists, duration, album) { lyrics ->
                            val filteredLyrics = LyricsUtils.filterLyricsCreditLines(lyrics)
                            val result = LyricsResult(provider.name, filteredLyrics)
                            synchronized(callbackMutex) {
                                allResult += result
                                callback(result)
                            }
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        reportException(e)
                    }
                }
            }
            otherJobs.forEach { it.join() }

            val otherLyricsCount = allResult.count { it.providerName != "LyricsPlus" }
            if (lyricsPlusProvider != null && otherLyricsCount <= 2) {
                launch {
                    try {
                        lyricsPlusProvider.getAllLyrics(context, mediaId, cleanedTitle, songArtists, duration, album) { lyrics ->
                            val filteredLyrics = LyricsUtils.filterLyricsCreditLines(lyrics)
                            val result = LyricsResult(lyricsPlusProvider.name, filteredLyrics)
                            synchronized(callbackMutex) {
                                allResult += result
                                callback(result)
                            }
                        }
                    } catch (e: CancellationException) {
                        throw e
                    } catch (e: Exception) {
                        reportException(e)
                    }
                }.join()
            }

            cache.put(cacheKey, allResult)
        }

        currentLyricsJob?.join()
    }

    private fun resolveLyricsProviders(preferences: androidx.datastore.preferences.core.Preferences): List<LyricsProvider> {
        val providerOrder = preferences[LyricsProviderOrderKey].orEmpty()
        if (providerOrder.isNotBlank()) {
            return LyricsProviderRegistry.getOrderedProviders(providerOrder)
        }

        return LyricsProviderRegistry.getDefaultProviderOrder()
            .mapNotNull { LyricsProviderRegistry.getProviderByName(it) }
    }

    companion object {
        private const val MAX_CACHE_SIZE = 3
    }
}

data class LyricsResult(
    val providerName: String,
    val lyrics: String,
)

data class LyricsWithProvider(
    val lyrics: String,
    val provider: String,
)
