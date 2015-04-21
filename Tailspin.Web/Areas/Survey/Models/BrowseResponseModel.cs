//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Areas.Survey.Models
{
    using Web.Survey.Shared.Models;

    public class BrowseResponseModel
    {
        public SurveyAnswer SurveyAnswer { get; set; }

        public string NextAnswerId { get; set; }

        public string PreviousAnswerId { get; set; }

        public bool CanMoveForward
        {
            get
            {
                return !string.IsNullOrEmpty(this.NextAnswerId);
            }
        }

        public bool CanMoveBackward
        {
            get
            {
                return !string.IsNullOrEmpty(this.PreviousAnswerId);
            }
        }
    }
}