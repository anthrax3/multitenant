//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Areas.Survey.Extensions
{
    using System.Collections.Specialized;
    using System.Web.Routing;

    internal static class RouteValueDictionaryExtensions
    {
        public static void MergeQueryToRouteValues(this RouteValueDictionary routeValues, NameValueCollection queryValues)
        {
            foreach (string key in queryValues.AllKeys)
            {
                routeValues[key] = queryValues[key];
            }
        }
    }
}