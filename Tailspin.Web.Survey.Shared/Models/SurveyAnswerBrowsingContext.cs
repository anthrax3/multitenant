//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Shared.Models
{
    public class SurveyAnswerBrowsingContext
    {
        public string PreviousId { get; set; }
        
        public string NextId { get; set; }
    }
}