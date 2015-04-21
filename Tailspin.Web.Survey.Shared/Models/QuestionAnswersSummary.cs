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

    public class QuestionAnswersSummary
    {
        public string AnswersSummary { get; set; }

        public QuestionType QuestionType { get; set; }

        public string QuestionText { get; set; }

        public string PossibleAnswers { get; set; }
    }
}