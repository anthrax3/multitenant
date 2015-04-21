//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Public.Utility
{
    using Shared.Models;

    public static class QuestionTemplateFactory
    {
        public static string Create(QuestionType questionType)
        {
            switch (questionType)
            {
                case QuestionType.SimpleText:
                    return "SimpleText";
                case QuestionType.MultipleChoice:
                    return "MultipleChoice";
                case QuestionType.FiveStars:
                    return "FiveStars";
                default:
                    return "String";
            }
        }
    }
}