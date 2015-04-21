//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Stores
{
    using System.Globalization;
    using Microsoft.Practices.Unity;
    using Tailspin.Web.Survey.Shared.Models;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;

    public class SurveyAnswerContainerFactory : ISurveyAnswerContainerFactory
    {
        private readonly IUnityContainer surveyAnswerBlobContainerResolver;

        public SurveyAnswerContainerFactory(IUnityContainer surveyAnswerBlobContainerResolver)
        {
            this.surveyAnswerBlobContainerResolver = surveyAnswerBlobContainerResolver;
        }

        public IAzureBlobContainer<SurveyAnswer> Create(string tenant, string surveySlug)
        {
            var containerName = string.Format(
                CultureInfo.InvariantCulture,
                "surveyanswers-{0}-{1}",
                tenant.ToLowerInvariant(),
                surveySlug.ToLowerInvariant());
            return this.surveyAnswerBlobContainerResolver.Resolve<IAzureBlobContainer<SurveyAnswer>>(
                new ParameterOverride("containerName", containerName));
        }
    }
}