// ==UserScript==
// @name         Rekata
// @namespace    rekata.zheng-she.com
// @version      1.0.4
// @description  Restore loan words from Katakana back to their original form
// @author       PythonShe
// @match        *://*/*
// @grant        GM_xmlhttpRequest
// @grant        GM_addStyle
// @grant        GM_getValue
// @grant        GM_setValue
// @grant        GM_registerMenuCommand
// @connect      translate.googleapis.com
// @connect      api-free.deepl.com
// @connect      *
// @run-at       document-idle
// @homepageURL  https://github.com/PythonShe/Rekata
// @supportURL   https://github.com/PythonShe/Rekata/issues
// ==/UserScript==

(function() {
    'use strict';

    var SCRIPT_NAME = 'Rekata';
    var STORAGE_KEY_SETTINGS = 'KT_SETTINGS_V1';
    var PANEL_HOST_ID = 'rekata-settings-host';
    var RUBY_CLASS = 'rekata-ruby';
    var RT_CLASS = 'rekata-rt';
    var SESSION_DATASET_KEY = 'ktSession';

    var SITE_SPECIFIC_STYLES = [
        /* YouTube — search results */
        '#video-title.ytd-video-renderer:has(ruby.' + RUBY_CLASS + ') {',
        '  line-height: 2.2rem;',
        '  max-height: 4.4rem;',
        '  -webkit-line-clamp: 2;',
        '  display: -webkit-box;',
        '  -webkit-box-orient: vertical;',
        '  overflow: hidden;',
        '  text-overflow: ellipsis;',
        '}',
        /* YouTube — homepage grid */
        '#video-title.ytd-rich-grid-media:has(ruby.' + RUBY_CLASS + ') {',
        '  line-height: 2.2rem;',
        '  max-height: 4.4rem;',
        '  -webkit-line-clamp: 2;',
        '  display: -webkit-box;',
        '  -webkit-box-orient: vertical;',
        '  overflow: hidden;',
        '  text-overflow: ellipsis;',
        '}',
        /* Bilibili — override card title line-height variable */
        '.bili-video-card:has(ruby.' + RUBY_CLASS + ') {',
        '  --title-line-height: 2.2rem;',
        '}',
        /* Bilibili — card title clamp */
        '.bili-video-card .bili-video-card__info--tit:has(ruby.' + RUBY_CLASS + ') {',
        '  -webkit-line-clamp: 2;',
        '  height: initial !important;',
        '}'
    ].join('\n');

    var KATAKANA_PATTERN = /[\u30A1-\u30FA\u30FD-\u30FF][\u3099\u309A\u30A1-\u30FF]*[\u3099\u309A\u30A1-\u30FA\u30FC-\u30FF]|[\uFF66-\uFF6F\uFF71-\uFF9D][\uFF65-\uFF9F]*[\uFF66-\uFF9F]/;
    var EXCLUDE_TAGS = {
        ruby: true,
        rt: true,
        rp: true,
        script: true,
        style: true,
        select: true,
        textarea: true,
        input: true,
        code: true,
        pre: true
    };

    var DEFAULT_SETTINGS = {
        enabled: true,
        backend: 'google',
        customEndpoint: '',
        deeplApiKey: '',
        blacklistPatterns: [],
        requestDebounceMs: 300,
        chunkSize: 120,
        cacheTtlMs: 7 * 24 * 60 * 60 * 1000,
        debug: false
    };

    var state = {
        settings: clone(DEFAULT_SETTINGS),
        isRunning: false,
        sessionId: 1,
        queue: new Map(),
        pendingNodes: new Set(),
        inFlightPhrases: new Set(),
        failedPhraseUntil: new Map(),
        dictionaryCache: new Map(),
        observer: null,
        processTimer: null,
        contextTimer: null,
        contextSnapshot: null,
        panel: null,
        nodeIdentity: {
            nextId: 1,
            map: new WeakMap()
        },
        youtubeSignalsBound: false,
        youtubeHandlers: [],
        lastForcedResetAt: 0,
        lastForcedResetUrl: ''
    };

    var log = {
        info: function(message, extra) {
            if (!state.settings.debug) {
                return;
            }
            if (typeof extra === 'undefined') {
                console.log('[' + SCRIPT_NAME + '] ' + message);
                return;
            }
            console.log('[' + SCRIPT_NAME + '] ' + message, extra);
        },
        warn: function(message, extra) {
            if (typeof extra === 'undefined') {
                console.warn('[' + SCRIPT_NAME + '] ' + message);
                return;
            }
            console.warn('[' + SCRIPT_NAME + '] ' + message, extra);
        },
        error: function(message, extra) {
            if (typeof extra === 'undefined') {
                console.error('[' + SCRIPT_NAME + '] ' + message);
                return;
            }
            console.error('[' + SCRIPT_NAME + '] ' + message, extra);
        }
    };

    function clone(value) {
        return JSON.parse(JSON.stringify(value));
    }

    function toInteger(value, fallback, min, max) {
        var parsed = Number(value);
        if (!Number.isFinite(parsed)) {
            parsed = fallback;
        }
        parsed = Math.round(parsed);
        if (Number.isFinite(min) && parsed < min) {
            parsed = min;
        }
        if (Number.isFinite(max) && parsed > max) {
            parsed = max;
        }
        return parsed;
    }

    function sanitizeSettings(raw) {
        var base = clone(DEFAULT_SETTINGS);
        var input = raw && typeof raw === 'object' ? raw : {};

        base.enabled = typeof input.enabled === 'boolean' ? input.enabled : DEFAULT_SETTINGS.enabled;
        base.backend = ['google', 'deepl', 'custom'].indexOf(input.backend) >= 0 ? input.backend : DEFAULT_SETTINGS.backend;
        base.customEndpoint = typeof input.customEndpoint === 'string' ? input.customEndpoint.trim() : DEFAULT_SETTINGS.customEndpoint;
        base.deeplApiKey = typeof input.deeplApiKey === 'string' ? input.deeplApiKey.trim() : DEFAULT_SETTINGS.deeplApiKey;
        base.blacklistPatterns = Array.isArray(input.blacklistPatterns)
            ? input.blacklistPatterns.filter(function(item) {
                return typeof item === 'string' && item.trim();
            }).map(function(item) {
                return item.trim();
            })
            : clone(DEFAULT_SETTINGS.blacklistPatterns);
        if (!base.blacklistPatterns.length) {
            base.blacklistPatterns = clone(DEFAULT_SETTINGS.blacklistPatterns);
        }
        base.requestDebounceMs = toInteger(input.requestDebounceMs, DEFAULT_SETTINGS.requestDebounceMs, 50, 3000);
        base.chunkSize = toInteger(input.chunkSize, DEFAULT_SETTINGS.chunkSize, 1, 500);
        base.cacheTtlMs = toInteger(input.cacheTtlMs, DEFAULT_SETTINGS.cacheTtlMs, 60 * 1000, 30 * 24 * 60 * 60 * 1000);
        base.debug = typeof input.debug === 'boolean' ? input.debug : DEFAULT_SETTINGS.debug;
        return base;
    }

    function gmGetValue(key, defaultValue) {
        try {
            if (typeof GM_getValue === 'function') {
                return Promise.resolve(GM_getValue(key, defaultValue));
            }
            if (typeof GM === 'object' && typeof GM.getValue === 'function') {
                return GM.getValue(key, defaultValue);
            }
        } catch (error) {
            log.warn('Failed to read GM value', error);
        }
        return Promise.resolve(defaultValue);
    }

    function gmSetValue(key, value) {
        try {
            if (typeof GM_setValue === 'function') {
                GM_setValue(key, value);
                return Promise.resolve();
            }
            if (typeof GM === 'object' && typeof GM.setValue === 'function') {
                return GM.setValue(key, value);
            }
        } catch (error) {
            log.warn('Failed to save GM value', error);
        }
        return Promise.resolve();
    }

    function gmAddStyle(cssText) {
        try {
            if (typeof GM_addStyle === 'function') {
                GM_addStyle(cssText);
                return;
            }
        } catch (error) {
            log.warn('GM_addStyle failed, using fallback', error);
        }
        var style = document.createElement('style');
        style.textContent = cssText;
        (document.head || document.documentElement).appendChild(style);
    }

    function gmRegisterMenuCommand(title, command) {
        if (typeof GM_registerMenuCommand === 'function') {
            GM_registerMenuCommand(title, command);
        } else if (typeof GM === 'object' && typeof GM.registerMenuCommand === 'function') {
            GM.registerMenuCommand(title, command);
        }
    }

    function gmRequest(details) {
        return new Promise(function(resolve, reject) {
            var requester = null;
            if (typeof GM_xmlhttpRequest === 'function') {
                requester = GM_xmlhttpRequest;
            } else if (typeof GM === 'object' && typeof GM.xmlHttpRequest === 'function') {
                requester = GM.xmlHttpRequest;
            }
            if (!requester) {
                reject(new Error('No GM_xmlhttpRequest implementation found.'));
                return;
            }

            requester({
                method: details.method,
                url: details.url,
                headers: details.headers || {},
                data: details.data,
                timeout: details.timeout || 15000,
                onload: function(response) {
                    if (response.status >= 200 && response.status < 300) {
                        resolve(response.responseText);
                    } else {
                        reject(new Error('HTTP ' + response.status + ' - ' + response.statusText));
                    }
                },
                onerror: function(error) {
                    reject(new Error('Network error: ' + String(error && error.error ? error.error : 'unknown')));
                },
                ontimeout: function() {
                    reject(new Error('Request timeout'));
                }
            });
        });
    }

    function wildcardToRegExp(pattern) {
        var escaped = pattern.replace(/[.+?^${}()|[\]\\]/g, '\\$&');
        return new RegExp('^' + escaped.replace(/\*/g, '.*') + '$');
    }

    function isBlacklistedUrl(url) {
        var patterns = state.settings.blacklistPatterns || [];
        for (var i = 0; i < patterns.length; i++) {
            var pattern = patterns[i];
            try {
                if (wildcardToRegExp(pattern).test(url)) {
                    return true;
                }
            } catch (error) {
                log.warn('Invalid blacklist pattern: ' + pattern);
            }
        }
        return false;
    }

    function splitPhrasesIntoChunks(phrases, chunkSize) {
        var chunks = [];
        for (var i = 0; i < phrases.length; i += chunkSize) {
            chunks.push(phrases.slice(i, i + chunkSize));
        }
        return chunks;
    }

    function parseGoogleResponse(responseText) {
        var parsed = null;
        try {
            parsed = JSON.parse(responseText);
        } catch (error) {
            throw new Error('Invalid Google response JSON');
        }
        var translatedMap = new Map();
        if (!Array.isArray(parsed) || !Array.isArray(parsed[0])) {
            return translatedMap;
        }

        parsed[0].forEach(function(item) {
            if (!Array.isArray(item)) {
                return;
            }
            var translated = typeof item[0] === 'string' ? item[0].trim() : '';
            var original = typeof item[1] === 'string' ? item[1].trim() : '';
            if (!translated || !original) {
                return;
            }
            translatedMap.set(original, translated);
        });
        return translatedMap;
    }

    function parseCustomResponse(responseText) {
        var parsed = null;
        try {
            parsed = JSON.parse(responseText);
        } catch (error) {
            throw new Error('Invalid custom adapter response JSON');
        }

        var translatedMap = new Map();
        if (!parsed || typeof parsed !== 'object') {
            return translatedMap;
        }

        if (parsed.translations && !Array.isArray(parsed.translations) && typeof parsed.translations === 'object') {
            Object.keys(parsed.translations).forEach(function(phrase) {
                var translated = parsed.translations[phrase];
                if (typeof translated === 'string' && translated.trim()) {
                    translatedMap.set(phrase, translated.trim());
                }
            });
            return translatedMap;
        }

        if (Array.isArray(parsed.translations)) {
            parsed.translations.forEach(function(item) {
                if (!item || typeof item !== 'object') {
                    return;
                }
                var phrase = typeof item.phrase === 'string' ? item.phrase.trim() : '';
                var translated = typeof item.translation === 'string' ? item.translation.trim() : '';
                if (phrase && translated) {
                    translatedMap.set(phrase, translated);
                }
            });
            return translatedMap;
        }

        Object.keys(parsed).forEach(function(key) {
            var value = parsed[key];
            if (typeof value === 'string' && value.trim()) {
                translatedMap.set(key, value.trim());
            }
        });
        return translatedMap;
    }

    function translateWithGoogle(phrases, context) {
        var joinedText = phrases.join('\n').replace(/\s+$/, '');
        var query = [
            'client=gtx',
            'dt=t',
            'sl=' + encodeURIComponent(context.sourceLang),
            'tl=' + encodeURIComponent(context.targetLang),
            'q=' + encodeURIComponent(joinedText)
        ].join('&');
        var url = 'https://translate.googleapis.com/translate_a/single?' + query;
        return gmRequest({
            method: 'GET',
            url: url
        }).then(parseGoogleResponse);
    }

    function translateWithDeepL(phrases, context) {
        var key = state.settings.deeplApiKey || '';
        if (!key) {
            return Promise.reject(new Error('DeepL API key is not configured. Please set it in Rekata settings.'));
        }

        var form = 'auth_key=' + encodeURIComponent(key)
            + '&source_lang=' + encodeURIComponent(context.sourceLang.toUpperCase())
            + '&target_lang=' + encodeURIComponent(context.targetLang.toUpperCase());
        phrases.forEach(function(phrase) {
            form += '&text=' + encodeURIComponent(phrase);
        });

        return gmRequest({
            method: 'POST',
            url: 'https://api-free.deepl.com/v2/translate',
            headers: {'Content-Type': 'application/x-www-form-urlencoded'},
            data: form
        }).then(function(responseText) {
            var parsed = JSON.parse(responseText);
            var translatedMap = new Map();
            if (!parsed || !Array.isArray(parsed.translations)) {
                return translatedMap;
            }
            parsed.translations.forEach(function(item, index) {
                if (!item || typeof item.text !== 'string') {
                    return;
                }
                var sourcePhrase = phrases[index];
                if (!sourcePhrase) {
                    return;
                }
                translatedMap.set(sourcePhrase, item.text.trim());
            });
            return translatedMap;
        });
    }

    function translateWithCustomBackend(phrases, context) {
        var endpoint = (state.settings.customEndpoint || '').trim();
        if (!endpoint) {
            return Promise.reject(new Error('Custom endpoint is not configured. Please set it in Rekata settings.'));
        }

        return gmRequest({
            method: 'POST',
            url: endpoint,
            headers: {'Content-Type': 'application/json'},
            data: JSON.stringify({
                sourceLang: context.sourceLang,
                targetLang: context.targetLang,
                phrases: phrases
            })
        }).then(parseCustomResponse);
    }

    function getTranslatorAdapter(backendName) {
        if (backendName === 'deepl') {
            return translateWithDeepL;
        }
        if (backendName === 'custom') {
            return translateWithCustomBackend;
        }
        return translateWithGoogle;
    }

    function getNodeIdentity(node) {
        if (!node) {
            return 0;
        }
        var id = state.nodeIdentity.map.get(node);
        if (!id) {
            id = state.nodeIdentity.nextId++;
            state.nodeIdentity.map.set(node, id);
        }
        return id;
    }

    function resolveMainContainerNode() {
        var selectors = [
            '#page-manager',
            'ytd-app #content',
            'main',
            '[role="main"]',
            '#main',
            '#app',
            '#root',
            'body'
        ];
        for (var i = 0; i < selectors.length; i++) {
            var found = document.querySelector(selectors[i]);
            if (found) {
                return found;
            }
        }
        return document.body;
    }

    function normalizeText(text) {
        return String(text || '')
            .replace(/\s+/g, ' ')
            .trim()
            .toLowerCase();
    }

    function collectFingerprintText(rootNode, maxFragments) {
        var fragments = [];
        fragments.push(document.title || '');

        if (rootNode && rootNode.querySelector) {
            var heading = rootNode.querySelector('h1, h2, [role="heading"]');
            if (heading && heading.textContent) {
                fragments.push(heading.textContent);
            }
        }

        var walkerRoot = rootNode || document.body;
        if (walkerRoot) {
            var walker = document.createTreeWalker(
                walkerRoot,
                NodeFilter.SHOW_TEXT,
                {
                    acceptNode: function(node) {
                        if (!node || !node.parentElement) {
                            return NodeFilter.FILTER_REJECT;
                        }
                        var tagName = node.parentElement.tagName ? node.parentElement.tagName.toLowerCase() : '';
                        if (EXCLUDE_TAGS[tagName]) {
                            return NodeFilter.FILTER_REJECT;
                        }
                        var value = normalizeText(node.nodeValue);
                        if (!value || value.length < 4) {
                            return NodeFilter.FILTER_REJECT;
                        }
                        return NodeFilter.FILTER_ACCEPT;
                    }
                }
            );

            while (fragments.length < maxFragments && walker.nextNode()) {
                fragments.push(walker.currentNode.nodeValue);
            }
        }

        return fragments.join(' ');
    }

    function tokenizeFingerprint(text) {
        var normalized = normalizeText(text);
        if (!normalized) {
            return [];
        }
        return normalized
            .split(/[^a-z0-9\u3040-\u30ff\u4e00-\u9fff]+/)
            .filter(function(token) {
                return token.length > 1;
            })
            .slice(0, 120);
    }

    function computeContextSnapshot() {
        var mainNode = resolveMainContainerNode();
        var fingerprintText = collectFingerprintText(mainNode, 7);
        return {
            url: location.href,
            mainNodeId: getNodeIdentity(mainNode),
            fingerprintTokens: tokenizeFingerprint(fingerprintText)
        };
    }

    function jaccardSimilarity(tokensA, tokensB) {
        if (!tokensA.length || !tokensB.length) {
            return 1;
        }
        var setA = new Set(tokensA);
        var setB = new Set(tokensB);
        var intersection = 0;

        setB.forEach(function(token) {
            if (setA.has(token)) {
                intersection++;
            }
        });

        var union = setA.size + setB.size - intersection;
        if (!union) {
            return 1;
        }
        return intersection / union;
    }

    function shouldResetByFingerprint(previousSnapshot, currentSnapshot) {
        var previousTokens = previousSnapshot.fingerprintTokens || [];
        var currentTokens = currentSnapshot.fingerprintTokens || [];
        if (previousTokens.length < 8 || currentTokens.length < 8) {
            return false;
        }
        return jaccardSimilarity(previousTokens, currentTokens) < 0.18;
    }

    function extractBaseTextFromRuby(rubyNode) {
        var text = '';
        var childNodes = rubyNode.childNodes;
        for (var i = 0; i < childNodes.length; i++) {
            var child = childNodes[i];
            if (child.nodeType === Node.TEXT_NODE) {
                text += child.nodeValue || '';
                continue;
            }
            if (child.nodeType !== Node.ELEMENT_NODE) {
                continue;
            }
            var tagName = child.tagName ? child.tagName.toLowerCase() : '';
            if (tagName === 'rt' || tagName === 'rp') {
                continue;
            }
            text += child.textContent || '';
        }
        return text;
    }

    function cleanupInjectedRuby(rootNode) {
        if (!rootNode || !rootNode.querySelectorAll) {
            return;
        }

        var parents = new Set();
        var injectedRubyNodes = rootNode.querySelectorAll('ruby.' + RUBY_CLASS);
        injectedRubyNodes.forEach(function(rubyNode) {
            if (!rubyNode || !rubyNode.parentNode) {
                return;
            }
            parents.add(rubyNode.parentNode);
            var baseText = extractBaseTextFromRuby(rubyNode);
            rubyNode.parentNode.replaceChild(document.createTextNode(baseText), rubyNode);
        });
        parents.forEach(function(parent) {
            if (parent.normalize) { parent.normalize(); }
        });
    }

    function clearSessionQueue() {
        state.queue.clear();
        state.pendingNodes.clear();
        state.inFlightPhrases.clear();
        state.failedPhraseUntil.clear();
    }

    function resetSession(reason, options) {
        var resetOptions = options || {};
        state.sessionId += 1;
        clearSessionQueue();
        if (resetOptions.cleanupDom !== false && document.body) {
            cleanupInjectedRuby(document.body);
        }
        if (document.body) {
            state.pendingNodes.add(document.body);
        }
        state.contextSnapshot = computeContextSnapshot();
        log.info('Session reset #' + state.sessionId + ' (' + reason + ')');
    }

    function detectAndHandleContextShift(trigger) {
        if (!state.isRunning) {
            return;
        }

        var previousSnapshot = state.contextSnapshot;
        var currentSnapshot = computeContextSnapshot();

        if (!previousSnapshot) {
            state.contextSnapshot = currentSnapshot;
            return;
        }

        var resetReason = '';
        if (previousSnapshot.url !== currentSnapshot.url) {
            resetReason = 'url-change';
        } else if (previousSnapshot.mainNodeId && currentSnapshot.mainNodeId && previousSnapshot.mainNodeId !== currentSnapshot.mainNodeId) {
            resetReason = 'main-container-replaced';
        } else if (shouldResetByFingerprint(previousSnapshot, currentSnapshot)) {
            resetReason = 'fingerprint-shift';
        }

        if (resetReason) {
            resetSession(resetReason + ':' + trigger);
            return;
        }

        state.contextSnapshot = currentSnapshot;
    }

    function getCachedTranslation(phrase) {
        var item = state.dictionaryCache.get(phrase);
        if (!item) {
            return null;
        }
        if (item.expiresAt <= Date.now()) {
            state.dictionaryCache.delete(phrase);
            return null;
        }
        return item.translation;
    }

    function setCachedTranslation(phrase, translation) {
        var sanitized = typeof translation === 'string' ? translation.trim() : '';
        if (!sanitized) {
            return;
        }
        state.dictionaryCache.set(phrase, {
            translation: sanitized,
            expiresAt: Date.now() + state.settings.cacheTtlMs
        });
    }

    function isRtNodeWritable(node) {
        return Boolean(
            node
            && node.nodeType === Node.ELEMENT_NODE
            && node.isConnected
            && node.dataset
            && node.dataset[SESSION_DATASET_KEY] === String(state.sessionId)
        );
    }

    function writeTranslationToNode(rtNode, translation) {
        if (!isRtNodeWritable(rtNode)) {
            return;
        }
        rtNode.dataset.rt = translation;
    }

    function applyTranslationToQueuedPhrase(phrase, translation) {
        var nodes = state.queue.get(phrase);
        if (!nodes || !nodes.size) {
            return;
        }
        nodes.forEach(function(node) {
            writeTranslationToNode(node, translation);
        });
        state.queue.delete(phrase);
    }

    function enqueueRtNode(phrase, rtNode) {
        var cached = getCachedTranslation(phrase);
        if (cached) {
            writeTranslationToNode(rtNode, cached);
            return;
        }

        var queued = state.queue.get(phrase);
        if (!queued) {
            queued = new Set();
            state.queue.set(phrase, queued);
        }
        queued.add(rtNode);
    }

    function isKatakanaText(text) {
        return KATAKANA_PATTERN.test(text || '');
    }

    function createRubyNode(phrase) {
        var ruby = document.createElement('ruby');
        ruby.className = RUBY_CLASS;
        ruby.appendChild(document.createTextNode(phrase));
        var rt = document.createElement('rt');
        rt.className = RT_CLASS;
        rt.dataset[SESSION_DATASET_KEY] = String(state.sessionId);
        ruby.appendChild(rt);
        return {ruby: ruby, rt: rt};
    }

    function injectRubyIntoTextNode(textNode) {
        if (!textNode || textNode.nodeType !== Node.TEXT_NODE || !textNode.parentNode) {
            return;
        }
        if (!textNode.nodeValue || !isKatakanaText(textNode.nodeValue)) {
            return;
        }

        var cursor = textNode;
        while (cursor && cursor.parentNode && cursor.nodeType === Node.TEXT_NODE) {
            var value = cursor.nodeValue || '';
            var match = KATAKANA_PATTERN.exec(value);
            if (!match) {
                break;
            }
            var phrase = match[0];
            var afterStart = cursor.splitText(match.index);
            // Remove empty text node artifact from splitText(0) — prevents
            // YouTube's title-reading from seeing "" as firstChild.nodeValue
            if (match.index === 0 && cursor.parentNode) {
                cursor.parentNode.removeChild(cursor);
            }
            var afterPhrase = afterStart.splitText(phrase.length);
            var created = createRubyNode(phrase);
            afterStart.parentNode.insertBefore(created.ruby, afterStart);
            afterStart.parentNode.removeChild(afterStart);
            enqueueRtNode(phrase, created.rt);
            cursor = afterPhrase;
        }
    }

    function shouldSkipElement(node) {
        if (!node || node.nodeType !== Node.ELEMENT_NODE) {
            return true;
        }
        if (node.id === PANEL_HOST_ID) {
            return true;
        }
        if (node.isContentEditable) {
            return true;
        }
        var tagName = node.tagName ? node.tagName.toLowerCase() : '';
        if (EXCLUDE_TAGS[tagName]) {
            return true;
        }
        return false;
    }

    function scanNode(node) {
        if (!node || !node.isConnected) {
            return;
        }

        if (node.nodeType === Node.TEXT_NODE) {
            if (!node.parentElement || shouldSkipElement(node.parentElement)) {
                return;
            }
            injectRubyIntoTextNode(node);
            return;
        }

        if (node.nodeType !== Node.ELEMENT_NODE) {
            return;
        }

        if (shouldSkipElement(node)) {
            return;
        }

        var textNodes = [];
        var walker = document.createTreeWalker(
            node,
            NodeFilter.SHOW_TEXT,
            {
                acceptNode: function(textNode) {
                    if (!textNode || !textNode.parentElement) {
                        return NodeFilter.FILTER_REJECT;
                    }
                    if (shouldSkipElement(textNode.parentElement)) {
                        return NodeFilter.FILTER_REJECT;
                    }
                    return NodeFilter.FILTER_ACCEPT;
                }
            }
        );

        while (walker.nextNode()) {
            textNodes.push(walker.currentNode);
        }
        textNodes.forEach(injectRubyIntoTextNode);
    }

    function scheduleProcessing(delay) {
        if (state.processTimer) {
            clearTimeout(state.processTimer);
        }
        state.processTimer = window.setTimeout(function() {
            state.processTimer = null;
            processPendingNodes();
        }, delay);
    }

    function processPendingNodes() {
        if (!state.isRunning) {
            return;
        }
        if (!document.body) {
            return;
        }
        if (isBlacklistedUrl(location.href)) {
            stopEngine('blacklisted-url');
            updatePanelSummary();
            return;
        }

        detectAndHandleContextShift('process');

        if (!state.pendingNodes.size) {
            flushTranslationQueue();
            return;
        }

        var nodes = Array.from(state.pendingNodes);
        state.pendingNodes.clear();

        nodes.forEach(scanNode);
        flushTranslationQueue();
    }

    function markPhrasesInFlight(phrases, isInFlight) {
        phrases.forEach(function(phrase) {
            if (isInFlight) {
                state.inFlightPhrases.add(phrase);
                return;
            }
            state.inFlightPhrases.delete(phrase);
        });
    }

    function requestTranslationChunk(phrases, requestSessionId) {
        var adapter = getTranslatorAdapter(state.settings.backend);
        markPhrasesInFlight(phrases, true);

        adapter(phrases, {sourceLang: 'ja', targetLang: 'en'})
            .then(function(translatedMap) {
                if (requestSessionId !== state.sessionId) {
                    log.info('Dropped stale translation response', {
                        responseSession: requestSessionId,
                        currentSession: state.sessionId
                    });
                    return;
                }

                phrases.forEach(function(phrase) {
                    var translated = translatedMap.get(phrase);
                    if (typeof translated === 'string' && translated.trim()) {
                        var value = translated.trim();
                        setCachedTranslation(phrase, value);
                        applyTranslationToQueuedPhrase(phrase, value);
                        state.failedPhraseUntil.delete(phrase);
                        return;
                    }
                    state.failedPhraseUntil.set(phrase, Date.now() + 30 * 1000);
                });
            })
            .catch(function(error) {
                log.error('Translation request failed', error);
                phrases.forEach(function(phrase) {
                    state.failedPhraseUntil.set(phrase, Date.now() + 30 * 1000);
                });
            })
            .finally(function() {
                markPhrasesInFlight(phrases, false);
            });
    }

    function flushTranslationQueue() {
        if (!state.isRunning) {
            return;
        }

        var now = Date.now();
        var requestablePhrases = [];

        state.queue.forEach(function(_nodes, phrase) {
            var cached = getCachedTranslation(phrase);
            if (cached) {
                applyTranslationToQueuedPhrase(phrase, cached);
                return;
            }

            if (state.inFlightPhrases.has(phrase)) {
                return;
            }

            var failedUntil = state.failedPhraseUntil.get(phrase) || 0;
            if (failedUntil > now) {
                return;
            }
            requestablePhrases.push(phrase);
        });

        if (!requestablePhrases.length) {
            return;
        }

        var chunks = splitPhrasesIntoChunks(requestablePhrases, state.settings.chunkSize);
        var requestSessionId = state.sessionId;
        chunks.forEach(function(chunk) {
            requestTranslationChunk(chunk, requestSessionId);
        });
    }

    function handleMutations(mutationList) {
        if (!state.isRunning) {
            return;
        }

        mutationList.forEach(function(record) {
            if (record.type === 'characterData') {
                state.pendingNodes.add(record.target);
                return;
            }

            if (record.type === 'childList') {
                record.addedNodes.forEach(function(node) {
                    state.pendingNodes.add(node);
                });
            }
        });

        scheduleProcessing(state.settings.requestDebounceMs);
    }

    function onPotentialNavigationSignal(source) {
        if (!state.isRunning) {
            return;
        }
        if (source.indexOf('youtube:') === 0) {
            var now = Date.now();
            if (state.lastForcedResetUrl !== location.href || now - state.lastForcedResetAt > 1000) {
                state.lastForcedResetUrl = location.href;
                state.lastForcedResetAt = now;
                resetSession('forced-navigation:' + source);
                scheduleProcessing(180);
                return;
            }
        }
        detectAndHandleContextShift(source);
        scheduleProcessing(0);
    }

    function bindYouTubeSignals() {
        if (state.youtubeSignalsBound) {
            return;
        }
        var events = ['yt-navigate-finish', 'yt-page-data-updated'];
        state.youtubeHandlers = events.map(function(eventName) {
            var handler = function() {
                onPotentialNavigationSignal('youtube:' + eventName);
            };
            window.addEventListener(eventName, handler, true);
            return {eventName: eventName, handler: handler};
        });
        state.youtubeSignalsBound = true;
    }

    function unbindYouTubeSignals() {
        if (!state.youtubeSignalsBound) {
            return;
        }
        state.youtubeHandlers.forEach(function(item) {
            window.removeEventListener(item.eventName, item.handler, true);
        });
        state.youtubeHandlers = [];
        state.youtubeSignalsBound = false;
    }

    function startEngine() {
        if (state.isRunning) {
            return;
        }
        if (!document.body) {
            return;
        }
        if (isBlacklistedUrl(location.href)) {
            log.info('Current URL is blacklisted. Engine stays stopped.');
            updatePanelSummary();
            return;
        }

        state.isRunning = true;
        resetSession('engine-start');

        state.observer = new MutationObserver(handleMutations);
        state.observer.observe(document.body, {
            childList: true,
            subtree: true,
            characterData: true
        });

        bindYouTubeSignals();
        state.contextTimer = window.setInterval(function() {
            if (!state.isRunning) {
                return;
            }
            detectAndHandleContextShift('interval');
            if (state.pendingNodes.size || state.queue.size) {
                scheduleProcessing(0);
            }
        }, 1200);

        state.pendingNodes.add(document.body);
        scheduleProcessing(0);
        updatePanelSummary();
        log.info('Engine started');
    }

    function stopEngine(reason) {
        if (!state.isRunning) {
            return;
        }
        state.isRunning = false;

        if (state.processTimer) {
            clearTimeout(state.processTimer);
            state.processTimer = null;
        }
        if (state.contextTimer) {
            clearInterval(state.contextTimer);
            state.contextTimer = null;
        }
        if (state.observer) {
            state.observer.disconnect();
            state.observer = null;
        }

        unbindYouTubeSignals();
        clearSessionQueue();
        if (document.body) {
            cleanupInjectedRuby(document.body);
        }
        updatePanelSummary();
        log.info('Engine stopped (' + reason + ')');
    }

    function evaluateEngineState() {
        var shouldRun = state.settings.enabled && !isBlacklistedUrl(location.href);
        if (shouldRun) {
            startEngine();
            return;
        }
        stopEngine('disabled-or-blacklisted');
    }

    function saveSettings() {
        return gmSetValue(STORAGE_KEY_SETTINGS, JSON.stringify(state.settings));
    }

    async function loadSettings() {
        var rawValue = await gmGetValue(STORAGE_KEY_SETTINGS, '');
        if (!rawValue) {
            state.settings = clone(DEFAULT_SETTINGS);
            return;
        }

        var parsed = null;
        try {
            parsed = JSON.parse(rawValue);
        } catch (error) {
            log.warn('Invalid settings data, fallback to default', error);
            state.settings = clone(DEFAULT_SETTINGS);
            return;
        }
        var sanitized = sanitizeSettings(parsed);
        state.settings = sanitized;
        var normalizedValue = JSON.stringify(sanitized);
        if (normalizedValue !== rawValue) {
            gmSetValue(STORAGE_KEY_SETTINGS, normalizedValue).catch(function(error) {
                log.warn('Failed to migrate settings format', error);
            });
        }
    }

    function buildPanel() {
        if (state.panel) {
            return state.panel;
        }

        var existingHost = document.getElementById(PANEL_HOST_ID);
        if (existingHost && existingHost.parentNode) {
            existingHost.parentNode.removeChild(existingHost);
        }

        var host = document.createElement('div');
        host.id = PANEL_HOST_ID;
        document.documentElement.appendChild(host);
        var shadow = host.attachShadow({mode: 'open'});

        shadow.innerHTML = [
            '<style>',
            ':host { all: initial; }',
            '.fab {',
            '  position: fixed;',
            '  right: 18px;',
            '  bottom: 18px;',
            '  z-index: 2147483640;',
            '  border: none;',
            '  border-radius: 999px;',
            '  padding: 10px 16px;',
            '  font: 600 13px/1.2 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;',
            '  color: #f8fafc;',
            '  background: linear-gradient(135deg, #0f766e, #1d4ed8);',
            '  cursor: pointer;',
            '  box-shadow: 0 8px 24px rgba(15, 23, 42, 0.25);',
            '}',
            '.fab[data-state="off"] {',
            '  background: linear-gradient(135deg, #475569, #334155);',
            '}',
            '.overlay {',
            '  position: fixed;',
            '  inset: 0;',
            '  z-index: 2147483641;',
            '  display: none;',
            '  align-items: center;',
            '  justify-content: center;',
            '  background: rgba(2, 6, 23, 0.45);',
            '  backdrop-filter: blur(2px);',
            '}',
            '.overlay.open { display: flex; }',
            '.panel {',
            '  width: min(760px, calc(100vw - 24px));',
            '  max-height: calc(100vh - 24px);',
            '  overflow: auto;',
            '  border-radius: 12px;',
            '  background: #f8fafc;',
            '  color: #0f172a;',
            '  border: 1px solid rgba(15, 23, 42, 0.12);',
            '  box-shadow: 0 20px 50px rgba(15, 23, 42, 0.28);',
            '  font: 500 13px/1.5 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;',
            '}',
            '.header {',
            '  display: flex;',
            '  justify-content: space-between;',
            '  align-items: center;',
            '  padding: 14px 16px;',
            '  background: linear-gradient(90deg, #134e4a, #1d4ed8);',
            '  color: #f8fafc;',
            '}',
            '.title { font-size: 14px; font-weight: 700; }',
            '.subtitle { font-size: 12px; opacity: 0.86; }',
            '.body { padding: 16px; display: grid; gap: 12px; }',
            '.row { display: grid; gap: 6px; }',
            '.row-inline {',
            '  display: grid;',
            '  grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));',
            '  gap: 10px;',
            '}',
            'label { font-size: 12px; font-weight: 700; color: #1e293b; }',
            'input[type="text"], input[type="password"], input[type="number"], select, textarea {',
            '  width: 100%;',
            '  box-sizing: border-box;',
            '  border: 1px solid #cbd5e1;',
            '  border-radius: 8px;',
            '  padding: 8px 10px;',
            '  font: 500 13px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace;',
            '  background: #ffffff;',
            '  color: #0f172a;',
            '}',
            'textarea { min-height: 110px; resize: vertical; }',
            '.checkbox { display: flex; align-items: center; gap: 8px; }',
            '.checkbox input { width: 16px; height: 16px; }',
            '.hint { color: #475569; font-size: 12px; }',
            '.status {',
            '  padding: 8px 10px;',
            '  border-radius: 8px;',
            '  background: #e2e8f0;',
            '  color: #0f172a;',
            '  font: 600 12px/1.4 ui-monospace, SFMono-Regular, Menlo, monospace;',
            '}',
            '.footer {',
            '  display: flex;',
            '  flex-wrap: wrap;',
            '  gap: 8px;',
            '  justify-content: flex-end;',
            '  padding: 14px 16px;',
            '  border-top: 1px solid #e2e8f0;',
            '}',
            '.btn {',
            '  border: 1px solid #cbd5e1;',
            '  border-radius: 8px;',
            '  background: #ffffff;',
            '  color: #0f172a;',
            '  padding: 8px 12px;',
            '  cursor: pointer;',
            '  font: 700 12px/1.2 -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;',
            '}',
            '.btn.primary {',
            '  border-color: #0f766e;',
            '  background: linear-gradient(135deg, #0f766e, #0f766e);',
            '  color: #f8fafc;',
            '}',
            '.btn.warn {',
            '  border-color: #b91c1c;',
            '  color: #b91c1c;',
            '}',
            '.close {',
            '  border: 1px solid rgba(248, 250, 252, 0.4);',
            '  background: transparent;',
            '  color: #f8fafc;',
            '  border-radius: 6px;',
            '  cursor: pointer;',
            '  padding: 4px 8px;',
            '}',
            '</style>',
            '<button class="fab" type="button">Rekata</button>',
            '<div class="overlay">',
            '  <section class="panel" role="dialog" aria-modal="true" aria-label="Rekata Settings">',
            '    <header class="header">',
            '      <div>',
            '        <div class="title">Rekata Settings</div>',
            '        <div class="subtitle">Restore Katakana with Ruby translations</div>',
            '      </div>',
            '      <button class="close" type="button">Close</button>',
            '    </header>',
            '    <div class="body">',
            '      <div class="status" data-role="status"></div>',
            '      <div class="row checkbox">',
            '        <input id="rk-enabled" data-role="enabled" type="checkbox" />',
            '        <label for="rk-enabled">Enable Rekata on current site scope</label>',
            '      </div>',
            '      <div class="row">',
            '        <label for="rk-backend">Translation Backend</label>',
            '        <select id="rk-backend" data-role="backend">',
            '          <option value="google">Google (No API key)</option>',
            '          <option value="deepl">DeepL</option>',
            '          <option value="custom">Custom Endpoint</option>',
            '        </select>',
            '      </div>',
            '      <div class="row-inline">',
            '        <div class="row">',
            '          <label for="rk-custom-endpoint">Custom Endpoint</label>',
            '          <input id="rk-custom-endpoint" data-role="custom-endpoint" type="text" placeholder="https://api.example.com/translate" />',
            '        </div>',
            '        <div class="row">',
            '          <label for="rk-deepl-key">DeepL API Key</label>',
            '          <input id="rk-deepl-key" data-role="deepl-key" type="password" placeholder="Enter DeepL API key" />',
            '        </div>',
            '      </div>',
            '      <div class="row-inline">',
            '        <div class="row">',
            '          <label for="rk-debounce">Request Debounce (ms)</label>',
            '          <input id="rk-debounce" data-role="debounce" type="number" min="50" max="3000" />',
            '        </div>',
            '        <div class="row">',
            '          <label for="rk-chunk-size">Chunk Size</label>',
            '          <input id="rk-chunk-size" data-role="chunk-size" type="number" min="1" max="500" />',
            '        </div>',
            '        <div class="row">',
            '          <label for="rk-cache-ttl">Cache TTL (ms)</label>',
            '          <input id="rk-cache-ttl" data-role="cache-ttl" type="number" min="60000" max="2592000000" />',
            '        </div>',
            '      </div>',
            '      <div class="row checkbox">',
            '        <input id="rk-debug" data-role="debug" type="checkbox" />',
            '        <label for="rk-debug">Enable debug logs</label>',
            '      </div>',
            '      <div class="row">',
            '        <label for="rk-blacklist">Blacklist Patterns (one per line)</label>',
            '        <textarea id="rk-blacklist" data-role="blacklist"></textarea>',
            '        <div class="hint">Wildcard supported. Example: *://*.example.com/*</div>',
            '      </div>',
            '    </div>',
            '    <footer class="footer">',
            '      <button class="btn" type="button" data-action="rebuild">Rebuild Session</button>',
            '      <button class="btn warn" type="button" data-action="defaults">Reset Defaults</button>',
            '      <button class="btn primary" type="button" data-action="save">Save</button>',
            '    </footer>',
            '  </section>',
            '</div>'
        ].join('');

        var panel = {
            host: host,
            shadow: shadow,
            fab: shadow.querySelector('.fab'),
            overlay: shadow.querySelector('.overlay'),
            status: shadow.querySelector('[data-role="status"]'),
            enabled: shadow.querySelector('[data-role="enabled"]'),
            backend: shadow.querySelector('[data-role="backend"]'),
            customEndpoint: shadow.querySelector('[data-role="custom-endpoint"]'),
            deeplKey: shadow.querySelector('[data-role="deepl-key"]'),
            debounce: shadow.querySelector('[data-role="debounce"]'),
            chunkSize: shadow.querySelector('[data-role="chunk-size"]'),
            cacheTtl: shadow.querySelector('[data-role="cache-ttl"]'),
            debug: shadow.querySelector('[data-role="debug"]'),
            blacklist: shadow.querySelector('[data-role="blacklist"]'),
            closeButton: shadow.querySelector('.close'),
            rebuildButton: shadow.querySelector('[data-action="rebuild"]'),
            defaultButton: shadow.querySelector('[data-action="defaults"]'),
            saveButton: shadow.querySelector('[data-action="save"]')
        };

        panel.fab.addEventListener('click', function() {
            openPanel();
        });
        panel.closeButton.addEventListener('click', function() {
            closePanel();
        });
        panel.overlay.addEventListener('click', function(event) {
            if (event.target === panel.overlay) {
                closePanel();
            }
        });

        panel.rebuildButton.addEventListener('click', function() {
            if (!state.settings.enabled) {
                return;
            }
            if (!state.isRunning) {
                evaluateEngineState();
                return;
            }
            resetSession('manual-rebuild');
            scheduleProcessing(0);
            updatePanelSummary();
        });

        panel.defaultButton.addEventListener('click', function() {
            applySettings(clone(DEFAULT_SETTINGS), true);
        });

        panel.saveButton.addEventListener('click', function() {
            var nextSettings = sanitizeSettings({
                enabled: panel.enabled.checked,
                backend: panel.backend.value,
                customEndpoint: panel.customEndpoint.value,
                deeplApiKey: panel.deeplKey.value,
                requestDebounceMs: panel.debounce.value,
                chunkSize: panel.chunkSize.value,
                cacheTtlMs: panel.cacheTtl.value,
                debug: panel.debug.checked,
                blacklistPatterns: panel.blacklist.value
                    .split('\n')
                    .map(function(line) {
                        return line.trim();
                    })
                    .filter(function(line) {
                        return Boolean(line);
                    })
            });
            applySettings(nextSettings, true);
            closePanel();
        });

        state.panel = panel;
        syncPanelInputs();
        updatePanelSummary();
        return panel;
    }

    function safeBuildPanel() {
        if (state.panel) {
            return state.panel;
        }
        try {
            return buildPanel();
        } catch (error) {
            log.warn('Panel UI is unavailable on this page. Engine continues to run.', error);
            var staleHost = document.getElementById(PANEL_HOST_ID);
            if (staleHost && staleHost.parentNode) {
                staleHost.parentNode.removeChild(staleHost);
            }
            return null;
        }
    }

    function syncPanelInputs() {
        if (!state.panel) {
            return;
        }
        state.panel.enabled.checked = state.settings.enabled;
        state.panel.backend.value = state.settings.backend;
        state.panel.customEndpoint.value = state.settings.customEndpoint;
        state.panel.deeplKey.value = state.settings.deeplApiKey;
        state.panel.debounce.value = String(state.settings.requestDebounceMs);
        state.panel.chunkSize.value = String(state.settings.chunkSize);
        state.panel.cacheTtl.value = String(state.settings.cacheTtlMs);
        state.panel.debug.checked = state.settings.debug;
        state.panel.blacklist.value = (state.settings.blacklistPatterns || []).join('\n');
        state.panel.fab.dataset.state = state.settings.enabled && !isBlacklistedUrl(location.href) ? 'on' : 'off';
    }

    function updatePanelSummary() {
        if (!state.panel) {
            return;
        }
        var scopeBlocked = isBlacklistedUrl(location.href);
        var status = state.settings.enabled ? 'enabled' : 'disabled';
        if (scopeBlocked) {
            status = 'blacklisted';
        }
        state.panel.status.textContent = [
            'status=' + status,
            'session=' + state.sessionId,
            'queued=' + state.queue.size,
            'inFlight=' + state.inFlightPhrases.size,
            'cache=' + state.dictionaryCache.size,
            'backend=' + state.settings.backend
        ].join(' | ');
        state.panel.fab.dataset.state = state.settings.enabled && !scopeBlocked ? 'on' : 'off';
    }

    function openPanel() {
        var panel = safeBuildPanel();
        if (!panel) {
            return;
        }
        syncPanelInputs();
        updatePanelSummary();
        panel.overlay.classList.add('open');
    }

    function closePanel() {
        if (!state.panel) {
            return;
        }
        state.panel.overlay.classList.remove('open');
    }

    function applySettings(nextSettings, persist) {
        state.settings = sanitizeSettings(nextSettings);
        syncPanelInputs();
        updatePanelSummary();

        if (persist) {
            saveSettings().catch(function(error) {
                log.error('Failed to save settings', error);
            });
        }

        evaluateEngineState();
        if (state.isRunning) {
            resetSession('settings-updated');
            scheduleProcessing(0);
        }
    }

    function registerMenuCommands() {
        gmRegisterMenuCommand('Rekata: Open Settings', function() {
            openPanel();
        });
        gmRegisterMenuCommand('Rekata: Toggle Enabled', function() {
            var nextSettings = clone(state.settings);
            nextSettings.enabled = !state.settings.enabled;
            applySettings(nextSettings, true);
        });
        gmRegisterMenuCommand('Rekata: Rebuild Session', function() {
            if (!state.settings.enabled) {
                return;
            }
            evaluateEngineState();
            if (state.isRunning) {
                resetSession('menu-rebuild');
                scheduleProcessing(0);
            }
            updatePanelSummary();
        });
    }

    function installBaseStyles() {
        gmAddStyle([
            'ruby.' + RUBY_CLASS + ' { ruby-position: over; }',
            'rt.' + RT_CLASS + '::before { content: attr(data-rt); }',
            SITE_SPECIFIC_STYLES
        ].join('\n'));
    }

    async function init() {
        installBaseStyles();
        log.info('boot:start');
        await loadSettings();
        registerMenuCommands();
        evaluateEngineState();
        safeBuildPanel();
        log.info('boot:done');
    }

    init().catch(function(error) {
        log.error('Initialization failed', error);
    });
})();
