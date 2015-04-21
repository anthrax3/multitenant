//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared
{
    public static class AzureConstants
    {
        public static class BlobContainers
        {
            public const string SurveyAnswers = "tempsurveyanswers";
            public const string SurveyAnswersSummaries = "surveyanswerssummaries";
            public const string SurveyAnswersLists = "surveyanswerslists";
            public const string Tenants = "tenants";
            public const string Logos = "logos";
        }

        public static class Queues
        {
            public const string SurveyAnswerStoredStandard = "surveyanswerstoredstandard";
            public const string SurveyAnswerStoredPremium = "surveyanswerstoredpremium";
            public const string SurveyTransferRequest = "surveytransfer";
        }

        public static class Tables
        {
            public const string Surveys = "Surveys";
            public const string Questions = "Questions";
        }
    }
}
