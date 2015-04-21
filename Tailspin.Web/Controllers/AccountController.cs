//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.Globalization;
    using System.IO;
    using System.Linq;
    using System.Web;
    using System.Web.Mvc;
    using Samples.Web.ClaimsUtillities;
    using Tailspin.Web.Security;
    using Tailspin.Web.Survey.Shared.DataExtensibility;
    using Tailspin.Web.Survey.Shared.Stores;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;

    [RequireHttps]
    [AuthenticateAndAuthorizeTenant(Roles = Tailspin.Roles.SurveyAdministrator)]
    public class AccountController : TenantController
    {
        private readonly IDictionary<string, Type> extendableTypes;
        private readonly IUDFDictionary udfDictionary;

        public AccountController(ITenantStore tenantStore, IUDFDictionary udfDictionary)
            : base(tenantStore)
        {
            this.udfDictionary = udfDictionary;
            this.extendableTypes = new Dictionary<string, Type>() { { "SurveyRow", typeof(SurveyRow) } };
        }

        public ActionResult Index()
        {
            var model = this.CreateTenantPageViewData(this.Tenant);
            model.Title = "My Account";
            return this.View(model);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult UploadLogo(string tenant, HttpPostedFileBase newLogo)
        {
            // TODO: Validate that the file received is an image
            if (newLogo != null && newLogo.ContentLength > 0)
            {
                this.TenantStore.UploadLogo(tenant, new BinaryReader(newLogo.InputStream).ReadBytes(Convert.ToInt32(newLogo.InputStream.Length)));
            }

            return this.RedirectToAction("Index");
        }

        public ActionResult ModelIndex()
        {
            var model = this.CreateTenantUdfPageViewData(null, this.extendableTypes.Keys.AsEnumerable());
            model.Title = "Model Extensions";
            return this.View(model);
        }

        public ActionResult UdfIndex(string tenant, string model)
        {
            var mvcModel = this.CreateTenantUdfPageViewData(model, this.udfDictionary.GetFieldsFor(tenant, this.extendableTypes[model]));
            mvcModel.Title = string.Format("User Defined Fields for {0} model", model);
            return this.View(mvcModel);
        }

        public ActionResult UdfNew(string tenant, string model)
        {
            var mvcModel = this.CreateTenantUdfPageViewData(model, new UDFMetadata());
            mvcModel.Title = string.Format("New User Defined Field for {0} model", model);
            return this.View(mvcModel);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult UdfNew(string tenant, string model, UDFMetadata metadata)
        {
            IEnumerable<UDFMetadataError> errorMessages;
            if (!this.udfDictionary.AddFieldFor(this.TenantName, this.extendableTypes[model], metadata, out errorMessages))
            {
                foreach (var error in errorMessages)
                {
                    switch (error.Field)
                    {
                        case UDFMetadataField.Name:
                            this.ModelState.AddModelError(
                                "ContentModel.Name",
                                string.Format(CultureInfo.InvariantCulture, "* {0}", error.Message));
                            break;
                        case UDFMetadataField.Display:
                            this.ModelState.AddModelError(
                                "ContentModel.Display",
                                string.Format(CultureInfo.InvariantCulture, "* {0}", error.Message));
                            break;
                    }
                }
            }

            if (!this.ModelState.IsValid)
            {
                var mvcModel = this.CreateTenantUdfPageViewData(model, metadata);
                mvcModel.Title = string.Format("New User Defined Field for {0} model : Error!", model);
                return this.View(mvcModel);
            }

            this.ViewBag.Model = model;
            return this.RedirectToAction("UdfIndex");
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult UdfDelete(string tenant, string model, string udfName)
        {
            this.udfDictionary.RemoveFieldFor(tenant, this.extendableTypes[model], udfName);
            return this.RedirectToAction("UdfIndex");
        }
    }
}