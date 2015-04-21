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

<%@ Page Language="C#" MasterPageFile="~/Views/Shared/Site.Master" Inherits="System.Web.Mvc.ViewPage<Tailspin.Web.Survey.Public.Models.TenantPageViewData<Tailspin.Web.Survey.Shared.Models.SurveyAnswer>>" %>

<%@ Import Namespace="Tailspin.Web.Survey.Public.Utility" %>
<asp:Content ID="Content2" ContentPlaceHolderID="MainContent" runat="server">
    <div id="surveyTitle">
        <h1>
            <%if (this.Model.ContentModel == null)
              { %>
            There was a problem locating the survey you requested. Please try again later.
            <%}
              else
              {%>
            <%:this.Model.ContentModel.Title%>
            <%} %>
        </h1>
    </div>
    <%if (this.Model.ContentModel != null)
      { %>
    <% using (Html.BeginForm())
       { %>
    <div id="surveyForm">
        <ol>
            <% for (int i = 0; i < this.Model.ContentModel.QuestionAnswers.Count; i++)
               { %>
            <li>
                <div class="questionText">
                    <%:this.Model.ContentModel.QuestionAnswers[i].QuestionText%>
                    <%:Html.ValidationMessageFor(m => m.ContentModel.QuestionAnswers[i].Answer)%>
                </div>
                <%: Html.EditorFor(m => m.ContentModel.QuestionAnswers[i], QuestionTemplateFactory.Create(Model.ContentModel.QuestionAnswers[i].QuestionType)) %>
            </li>
            <% } %>
        </ol>
        <input id="finish" type="submit" value="Finish" class="bigOrangeButton" />
    </div>
    <% } %>
    <script src="<%:Url.Content("~/Scripts/jquery-1.4.1.min.js")%>" language="javascript" type="text/javascript"></script>
    <script src="<%:Url.Content("~/Scripts/jquery.rating.js")%>" language="javascript" type="text/javascript"></script>
    <%} %>
</asp:Content>
