//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Areas.Survey.Controllers
{
    using System;
    using System.Collections.Generic;
    using System.Data.Services.Client;
    using System.Globalization;
    using System.Web.Mvc;
    using Models;
    using Samples.Web.ClaimsUtillities;
    using Tailspin.Web.Controllers;
    using Tailspin.Web.Security;
    using Tailspin.Web.Survey.Shared.DataExtensibility;
    using Tailspin.Web.Survey.Shared.Helpers;
    using Tailspin.Web.Survey.Shared.Models;
    using Tailspin.Web.Survey.Shared.Stores;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;

    [RequireHttps]
    [AuthenticateAndAuthorizeTenant(Roles = Tailspin.Roles.SurveyAdministrator)]
    public class SurveysController : TenantController
    {
        public const string CachedSurvey = "cachedSurvey";

        private readonly ISurveyStore surveyStore;
        private readonly ISurveyAnswerStore surveyAnswerStore;
        private readonly ISurveyAnswersSummaryStore surveyAnswersSummaryStore;
        private readonly ISurveyTransferStore surveyTransferStore;
        private readonly IUDFDictionary udfDictionary;

        public SurveysController(
            ISurveyStore surveyStore,
            ISurveyAnswerStore surveyAnswerStore,
            ISurveyAnswersSummaryStore surveyAnswersSummaryStore,
            ITenantStore tenantStore,
            ISurveyTransferStore surveyTransferStore,
            IUDFDictionary udfDictionary)
            : base(tenantStore)
        {
            this.surveyStore = surveyStore;
            this.surveyAnswerStore = surveyAnswerStore;
            this.surveyAnswersSummaryStore = surveyAnswersSummaryStore;
            this.surveyTransferStore = surveyTransferStore;
            this.udfDictionary = udfDictionary;
        }

        [HttpGet]
        public ActionResult Index()
        {
            this.TempData[CachedSurvey] = null;

            IEnumerable<Survey> surveysForTenant = this.surveyStore.GetSurveysByTenant(this.TenantName);

            var model = this.CreateTenantPageViewData(surveysForTenant);
            model.Title = "My Surveys";

            return this.View(model);
        }

        [HttpGet]
        public ActionResult New()
        {
            var cachedSurvey = (Survey)this.TempData[CachedSurvey];

            if (cachedSurvey == null || 
                !this.udfDictionary.AreValidFor<SurveyRow>(this.TenantName, cachedSurvey.UserDefinedFields))
            {
                // First time to the page
                cachedSurvey = new Survey()
                {
                    UserDefinedFields = this.udfDictionary.InstanceFieldsFor<SurveyRow>(this.TenantName)
                };
            }

            var model = this.CreateTenantPageViewData(cachedSurvey);
            model.Title = "New Survey";

            this.TempData[CachedSurvey] = cachedSurvey;

            return this.View(model);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult New(Survey contentModel)
        {
            var cachedSurvey = (Survey)this.TempData[CachedSurvey];

            if (cachedSurvey == null)
            {
                return this.RedirectToAction("New");
            }

            cachedSurvey.Title = contentModel.Title;

            if (cachedSurvey.Questions == null || cachedSurvey.Questions.Count <= 0)
            {
                this.ModelState.AddModelError("ContentModel.Questions", string.Format(CultureInfo.InvariantCulture, "Please add at least one question to the survey."));
            }

            if ((cachedSurvey.UserDefinedFields != null) &&
                (cachedSurvey.UserDefinedFields.Count != contentModel.UserDefinedFields.Count))
            {
                this.ModelState.AddModelError("ContentModel.UserDefinedFields", string.Format(CultureInfo.InvariantCulture, "The number of custom properties don't match with the custom model."));
            }

            if (cachedSurvey.UserDefinedFields != null)
            {
                for (int i = 0; i < cachedSurvey.UserDefinedFields.Count; i++)
                {
                    var normalizedValue = ((string[])contentModel.UserDefinedFields[i].Value)[0];
                    if (cachedSurvey.UserDefinedFields[i].Mandatory && string.IsNullOrWhiteSpace(normalizedValue))
                    {
                        this.ModelState.AddModelError(
                            string.Format("ContentModel.UserDefinedFields[{0}].Value", i),
                            string.Format(CultureInfo.InvariantCulture, "* You must provide a value for extended property '{0}'.", cachedSurvey.UserDefinedFields[i].Display));
                    }
                    try
                    {
                        cachedSurvey.UserDefinedFields[i].SetUDFValue(normalizedValue);
                    }
                    catch (Exception ex)
                    {
                        this.ModelState.AddModelError(
                            string.Format("ContentModel.UserDefinedFields[{0}].Value", i), 
                            string.Format(CultureInfo.InvariantCulture, "* Invalid value for extended property '{0}': {1}.", cachedSurvey.UserDefinedFields[i].Display, ex.Message));
                    }
                }
            }

            contentModel.UserDefinedFields = cachedSurvey.UserDefinedFields;
            contentModel.Questions = cachedSurvey.Questions;

            if (!this.ModelState.IsValid)
            {
                var model = this.CreateTenantPageViewData(contentModel);
                model.Title = "New Survey";
                this.TempData[CachedSurvey] = cachedSurvey;
                return this.View(model);
            }

            contentModel.Tenant = this.TenantName;
            try
            {
                this.surveyStore.SaveSurvey(contentModel);
            }
            catch (DataServiceRequestException ex)
            {
                var dataServiceClientException = ex.InnerException as DataServiceClientException;
                if (dataServiceClientException != null)
                {
                    if (dataServiceClientException.StatusCode == 409)
                    {
                        TraceHelper.TraceWarning(ex.TraceInformation());

                        var model = this.CreateTenantPageViewData(contentModel);
                        model.Title = "New Survey";
                        this.ModelState.AddModelError("ContentModel.Title", string.Format(CultureInfo.InvariantCulture, "The name '{0}' is already in use. Please choose another name.", model.ContentModel.Title));
                        return this.View(model);
                    }
                }

                TraceHelper.TraceError(ex.TraceInformation());

                throw;
            }

            this.TempData.Remove(CachedSurvey);
            return this.RedirectToAction("Index");
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult NewQuestion(Survey contentModel)
        {
            var cachedSurvey = (Survey)this.TempData[CachedSurvey];

            if (cachedSurvey == null)
            {
                return this.RedirectToAction("New");
            }

            cachedSurvey.Title = contentModel.Title;

            if ((cachedSurvey.UserDefinedFields != null) &&
                (cachedSurvey.UserDefinedFields.Count != contentModel.UserDefinedFields.Count))
            {
                this.ModelState.AddModelError("ContentModel.UserDefinedFields", string.Format(CultureInfo.InvariantCulture, "The number of custom properties don't match with the custom model."));
            }

            if (cachedSurvey.UserDefinedFields != null)
            {
                for (int i = 0; i < cachedSurvey.UserDefinedFields.Count; i++)
                {
                    var normalizedValue = ((string[])contentModel.UserDefinedFields[i].Value)[0];
                    cachedSurvey.UserDefinedFields[i].Value = normalizedValue;
                }
            }

            this.TempData[CachedSurvey] = cachedSurvey;

            var model = this.CreateTenantPageViewData(new Question());
            model.Title = "New Question";

            return this.View(model);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult AddQuestion(Question contentModel)
        {
            var cachedSurvey = (Survey)this.TempData[CachedSurvey];

            if (!this.ModelState.IsValid)
            {
                this.TempData[CachedSurvey] = cachedSurvey;
                var model = this.CreateTenantPageViewData(contentModel ?? new Question());
                model.Title = "New Question";
                return this.View("NewQuestion", model);
            }

            if (contentModel.PossibleAnswers != null)
            {
                contentModel.PossibleAnswers = contentModel.PossibleAnswers.Replace("\r\n", "\n");
            }

            cachedSurvey.Questions.Add(contentModel);
            this.TempData[CachedSurvey] = cachedSurvey;
            return this.RedirectToAction("New");
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult Delete(string tenant, string surveySlug)
        {
            this.surveyStore.DeleteSurveyByTenantAndSlugName(tenant, surveySlug);
            this.surveyAnswerStore.DeleteSurveyAnswers(tenant, surveySlug);
            this.surveyAnswersSummaryStore.DeleteSurveyAnswersSummary(tenant, surveySlug);

            return this.RedirectToAction("Index");
        }

        [HttpGet]
        public ActionResult Analyze(string tenant, string surveySlug)
        {
            var surveyAnswersSummary = this.surveyAnswersSummaryStore.GetSurveyAnswersSummary(tenant, surveySlug);

            var model = this.CreateTenantPageViewData(surveyAnswersSummary);
            model.Title = surveySlug;
            return this.View(model);
        }

        [HttpGet]
        public ActionResult BrowseResponses(string tenant, string surveySlug, string answerId)
        {
            SurveyAnswer surveyAnswer = null;
            if (string.IsNullOrEmpty(answerId))
            {
                answerId = this.surveyAnswerStore.GetFirstSurveyAnswerId(tenant, surveySlug);
            }

            if (!string.IsNullOrEmpty(answerId))
            {
                surveyAnswer = this.surveyAnswerStore.GetSurveyAnswer(tenant, surveySlug, answerId);
            }

            var surveyAnswerBrowsingContext = this.surveyAnswerStore.GetSurveyAnswerBrowsingContext(tenant, surveySlug, answerId);

            var browseResponsesModel = new BrowseResponseModel
                                           {
                                               SurveyAnswer = surveyAnswer,
                                               PreviousAnswerId = surveyAnswerBrowsingContext.PreviousId,
                                               NextAnswerId = surveyAnswerBrowsingContext.NextId
                                           };

            var model = this.CreateTenantPageViewData(browseResponsesModel);
            model.Title = surveySlug;
            return this.View(model);
        }

        [HttpGet]
        public ActionResult ExportResponses(string surveySlug)
        {
            var exportResponseModel = new ExportResponseModel { Tenant = this.Tenant };
            string answerId = this.surveyAnswerStore.GetFirstSurveyAnswerId(this.TenantName, surveySlug);
            exportResponseModel.HasResponses = !string.IsNullOrEmpty(answerId);

            var model = this.CreateTenantPageViewData(exportResponseModel);
            model.Title = surveySlug;
            return this.View(model);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public ActionResult ExportResponses(string tenant, string surveySlug)
        {
            this.surveyTransferStore.Transfer(tenant, surveySlug);
            return this.RedirectToAction("BrowseResponses");
        }
    }
}