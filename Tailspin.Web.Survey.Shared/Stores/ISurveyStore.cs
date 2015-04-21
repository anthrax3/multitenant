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
    using System.Collections.Generic;
    using Tailspin.Web.Survey.Shared.Models;

    public interface ISurveyStore
    {
        void Initialize();
        void SaveSurvey(Survey survey);
        void DeleteSurveyByTenantAndSlugName(string tenant, string slugName);
        Survey GetSurveyByTenantAndSlugName(string tenant, string slugName, bool getQuestions);
        IEnumerable<Survey> GetSurveysByTenant(string tenant);
        IEnumerable<Survey> GetRecentSurveys();
    }
}