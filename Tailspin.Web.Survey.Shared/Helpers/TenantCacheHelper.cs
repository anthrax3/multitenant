//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Helpers
{
    using System;
    using System.Linq;
    using Microsoft.ApplicationServer.Caching;
    using Microsoft.Practices.TransientFaultHandling;
    using Tailspin.Web.Survey.Shared.Stores.Azure;

    internal static class TenantCacheHelper
    {
        private static readonly DataCacheFactory CacheFactory;
        private static readonly IRetryPolicyFactory RetryPolicyFactory;

        static TenantCacheHelper()
        {
            RetryPolicyFactory = new ConfiguredRetryPolicyFactory();
            CacheFactory = GetRetryPolicy().ExecuteAction<DataCacheFactory>(() => new DataCacheFactory());
        }

        internal static void AddToCache<T>(string tenant, string key, T @object) where T : class
        {
            GetRetryPolicy().ExecuteAction(() =>
                {
                    DataCache cache = CacheFactory.GetDefaultCache();
                    if (!cache.GetSystemRegions().Contains(tenant.ToLowerInvariant()))
                    {
                        cache.CreateRegion(tenant.ToLowerInvariant());
                    }
                    cache.Put(key.ToLowerInvariant(), @object, tenant.ToLowerInvariant());
                });
        }

        internal static T GetFromCache<T>(string tenant, string key, Func<T> @default) where T : class
        {
            return GetRetryPolicy().ExecuteAction<T>(() =>
                {
                    var result = default(T);

                    var success = false;
                    DataCache cache = CacheFactory.GetDefaultCache();
                    result = cache.Get(key.ToLowerInvariant(), tenant.ToLowerInvariant()) as T;
                    if (result != null)
                    {
                        success = true;
                    }
                    else if (@default != null)
                    {
                        result = @default();
                        if (result != null)
                        {
                            AddToCache(tenant.ToLowerInvariant(), key.ToLowerInvariant(), result);
                        }
                    }
                    TraceHelper.TraceInformation("cache {2} for {0} [{1}]", key, tenant, success ? "hit" : "miss");

                    return result;
                });
        }

        internal static void RemoveFromCache(string tenant, string key)
        {
            GetRetryPolicy().ExecuteAction(() =>
                {
                    DataCache cache = CacheFactory.GetDefaultCache();
                    cache.Remove(key.ToLowerInvariant(), tenant.ToLowerInvariant());
                });
        }

        internal static void RemoveAllFromCache(string tenant)
        {
            GetRetryPolicy().ExecuteAction(() =>
            {
                DataCache cache = CacheFactory.GetDefaultCache();
                cache.RemoveRegion(tenant.ToLowerInvariant());
            });
        }

        private static RetryPolicy GetRetryPolicy()
        {
            var retryPolicy = RetryPolicyFactory.GetDefaultAzureCachingRetryPolicy();
            retryPolicy.Retrying += new EventHandler<RetryingEventArgs>((sender, args) =>
                {
                    var msg = string.Format(
                        "Caching Retry - Count:{0}, Delay:{1}, Exception:{2}",
                        args.CurrentRetryCount,
                        args.Delay,
                        args.LastException.TraceInformation());
                    TraceHelper.TraceWarning(msg);
                });
            return retryPolicy;
        }
    }
}
