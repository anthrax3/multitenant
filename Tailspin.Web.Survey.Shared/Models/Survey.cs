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
    using System;
    using System.Collections.Generic;
    using System.ComponentModel.DataAnnotations;
    using Tailspin.Web.Survey.Shared.DataExtensibility;

    [Serializable]
    public class Survey : IUDFModel
    {
        private readonly string slugName;

        public Survey()
            : this(string.Empty)
        {
        }

        public Survey(string slugName)
        {
            this.slugName = slugName;
            this.Questions = new List<Question>();
        }

        public string SlugName
        {
            get
            {
                return this.slugName;
            }
        }

        public string Tenant { get; set; }

        [Required(ErrorMessage = "* You must provide a Title for the survey.")]
        public string Title { get; set; }

        public DateTime CreatedOn { get; set; }

        public List<Question> Questions { get; set; }

        public IList<UDFItem> UserDefinedFields { get; set; }
    }
}