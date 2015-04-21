//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web
{
    using System.Web.Mvc;
    using System.Web.Routing;

    public static class AppRoutes
    {
        public static void RegisterRoutes(RouteCollection routes)
        {
            routes.MapRoute(
                "OnBoarding",
                string.Empty,
                new { controller = "OnBoarding", action = "Index" });

            routes.MapRoute(
                "Management",
                "Management",
                new { controller = "Management", action = "Index" });

            routes.MapRoute(
                "Management-New",
                "Management/new",
                new { controller = "Management", action = "New" });

            routes.MapRoute(                
                "Management-Detail",        
                "Management/{tenant}",
                new { controller = "Management", action = "Detail" });

            routes.MapRoute(
               "JoinTenant",
               "Join",
               new { controller = "OnBoarding", action = "Join" });

            routes.MapRoute(
                "FederationResultProcessing",
                "FederationResult",
                new { controller = "ClaimsAuthentication", action = "FederationResult" });

            routes.MapRoute(
                "FederatedSignout",
                "Signout",
                new { controller = "ClaimsAuthentication", action = "Signout" });

            routes.MapRoute(
                "MyAccount",
                "{tenant}/MyAccount",
                new { controller = "Account", action = "Index" });

            routes.MapRoute(
                "UploadLogo",
                "{tenant}/MyAccount/UploadLogo",
                new { controller = "Account", action = "UploadLogo" });

            routes.MapRoute(
                "ModelIndex",
                "{tenant}/Udf",
                new { controller = "Account", action = "ModelIndex" });

            routes.MapRoute(
                "UdfIndex",
                "{tenant}/Udf/{model}",
                new { controller = "Account", action = "UdfIndex" });

            routes.MapRoute(
                "UdfNew",
                "{tenant}/Udf/{model}/New",
                new { controller = "Account", action = "UdfNew" });

            routes.MapRoute(
                "UdfDelete",
                "{tenant}/Udf/{model}/{udfName}/Delete",
                new { controller = "Account", action = "UdfDelete" });
        }
    }
}
