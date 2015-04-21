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
    using System.Collections.Generic;
    using Models;

    public interface ISurveyAnswerStore
    {
        void Initialize();
        void SaveSurveyAnswer(SurveyAnswer surveyAnswer);
        SurveyAnswer GetSurveyAnswer(string tenant, string slugName, string surveyAnswerId);
        string GetFirstSurveyAnswerId(string tenant, string slugName);
        void AppendSurveyAnswerIdsToAnswersList(string tenant, string slugName, IEnumerable<string> surveyAnswerIds);
        SurveyAnswerBrowsingContext GetSurveyAnswerBrowsingContext(string tenant, string slugName, string answerId);
        IEnumerable<string> GetSurveyAnswerIds(string tenant, string slugName);
        void DeleteSurveyAnswers(string tenant, string slugName);
    }
}