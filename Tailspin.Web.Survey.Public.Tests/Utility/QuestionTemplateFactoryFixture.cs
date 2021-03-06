﻿//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Survey.Public.Tests.Utility
{
    using Microsoft.VisualStudio.TestTools.UnitTesting;
    using Tailspin.Web.Survey.Public.Utility;
    using Tailspin.Web.Survey.Shared.Models;

    [TestClass]
    public class QuestionTemplateFactoryFixture
    {
        [TestMethod]
        public void CreateForSimpleText()
        {
            Assert.AreEqual(QuestionType.SimpleText.ToString(), QuestionTemplateFactory.Create(QuestionType.SimpleText));
        }

        [TestMethod]
        public void CreateForMultipleChoice()
        {
            Assert.AreEqual(QuestionType.MultipleChoice.ToString(), QuestionTemplateFactory.Create(QuestionType.MultipleChoice));
        }

        [TestMethod]
        public void CreateForFiveStars()
        {
            Assert.AreEqual(QuestionType.FiveStars.ToString(), QuestionTemplateFactory.Create(QuestionType.FiveStars));
        }
    }
}
