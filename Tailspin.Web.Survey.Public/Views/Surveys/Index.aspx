<%--
===============================================================================
 Microsoft patterns & practices
 Windows Azure Architecture Guide
===============================================================================
 Copyright © Microsoft Corporation.  All rights reserved.
 This code released under the terms of the 
 Microsoft patterns & practices license (http://wag.codeplex.com/license)
===============================================================================
--%>

<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<Tailspin.Web.Survey.Public.Models.TenantPageViewData<IEnumerable<Tailspin.Web.Survey.Shared.Models.Survey>>>" %>

<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">
    <div id="surveyTitle">
        <h1>
            Existing surveys
        </h1>
    </div>

    <% if (this.Model.ContentModel.Count() == 0) { %>
    <div class="noResults">
        <h3>No surveys have been added yet.</h3>
    </div>
    <% } else { %>
    <div id="surveys">
        <ul>
            <% foreach (var survey in this.Model.ContentModel) %>
            <% { %>
            <li>
                <%:Html.ActionLink(survey.Title, "Display", "Surveys", new { tenant = survey.Tenant, surveySlug = survey.SlugName }, null)%>
                (<%:survey.Tenant%>)
            </li>
            <% } %>
        </ul>
    </div>
    <% } %>
</asp:Content>
