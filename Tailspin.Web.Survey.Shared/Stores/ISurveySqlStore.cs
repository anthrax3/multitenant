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
    using Models;

    public interface ISurveySqlStore
    {
        void SaveSurvey(string connectionString, SurveyData surveyData);
        void Reset(string connectionString, string tenant, string slugName);
    }
}