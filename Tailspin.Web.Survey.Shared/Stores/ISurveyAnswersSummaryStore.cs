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
    using System;
    using Models;
    using Tailspin.Web.Survey.Shared.Stores.AzureStorage;

    public interface ISurveyAnswersSummaryStore
    {
        void Initialize();
        SurveyAnswersSummary GetSurveyAnswersSummary(string tenant, string slugName);        
        void DeleteSurveyAnswersSummary(string tenant, string slugName);
        void SaveSurveyAnswersSummary(SurveyAnswersSummary surveyAnswersSummary);
        void MergeSurveyAnswersSummary(SurveyAnswersSummary partialSurveyAnswersSummary);
    }
}
