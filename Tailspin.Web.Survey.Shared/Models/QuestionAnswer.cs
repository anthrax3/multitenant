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
    using System.ComponentModel.DataAnnotations;

    public class QuestionAnswer
    {
        public string QuestionText { get; set; }

        public QuestionType QuestionType { get; set; }
        
        [Required(ErrorMessage = "* You must provide an answer.")]
        public string Answer { get; set; }

        public string PossibleAnswers { get; set; }
    }
}