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
    using System;
    using System.Collections.Generic;
    using System.Linq;

    public static class SummaryExtensions
    {
        public static int Total(this Dictionary<string, int> values)
        {
            if (values == null)
            {
                return 0;
            }

            return values.Values.Sum();
        }

        public static int PercentOf(this int value, int total)
        {
            if (total == 0)
            {
                return 0;
            }

            decimal percent = Convert.ToDecimal(value) / Convert.ToDecimal(total);
            decimal smallPercent = Math.Round(percent, 2) * 100;
            int fullPercent = int.Parse(Math.Round(smallPercent, 0).ToString());
            return fullPercent;
        }
    }
}