//===============================================================================
// Microsoft patterns & practices
// Windows Azure Architecture Guide
//===============================================================================
// Copyright © Microsoft Corporation.  All rights reserved.
// This code released under the terms of the 
// Microsoft patterns & practices license (http://wag.codeplex.com/license)
//===============================================================================


namespace Tailspin.Web.Utility
{
    public static class StringExtensions
    {
        public static string Capitalize(this string word)
        {
            if (string.IsNullOrWhiteSpace(word))
            {
                return word;
            }

            return word[0].ToString().ToUpperInvariant() + word.Substring(1);
        }
    }
}
