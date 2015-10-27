#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Urls for backend ember-django application.

@author: e-science Dev-team
"""

from django.conf import settings
from django.conf.urls import patterns, include, url
from django.contrib import admin
from views import SessionView, StatusView, JobsView, HdfsView, MainPageView, SettingsView, \
StatisticsView, NewsView, FaqView, OrkaImagesView, VreServerView, VreImagesView, DslView, ClustersView
admin.site.site_header = "GRNET e-Science Administration"
admin.site.site_title = admin.site.site_header
admin.site.index_title = ''

urlpatterns = patterns('', url(r'^$', MainPageView.as_view()),
                       url(r'^api/statistics', StatisticsView.as_view(), name='statistics_of_service'),
                       url(r'^api/newsitems', NewsView.as_view(), name='news_list'),
                       url(r'^api/faqitems', FaqView.as_view()),
                       url(r'^api/orkaimages', OrkaImagesView.as_view(), name='orka_images'),
                       url(r'^api/vreimages', VreImagesView.as_view(), name='vre_images'),
                       url(r'^admin', include(admin.site.urls)),
                       url(r'^api/users', SessionView.as_view(), name='user_status'),
                       url(r'^api/clusters', ClustersView.as_view(), name='hadoop_clusters'),
                       url(r'^api/clusterchoices', StatusView.as_view(), name='cluster_choices'),
                       url(r'^api/jobs', JobsView.as_view(), name='tasks_list'),
                       url(r'^api/vreservers', VreServerView.as_view(), name='virtual_research_environments'),
                       url(r'^api/dsls', DslView.as_view(), name='reproducible_experiements'),
                       url(r'^api/hdfs', HdfsView.as_view(), name='hdfs_actions'),
                       url(r'^api/settings', SettingsView.as_view(), name='settings')
                       )

# if settings.DEBUG:
#     import debug_toolbar
#     urlpatterns += patterns('',
#         url(r'^__debug__/', include(debug_toolbar.urls)),
#     )
