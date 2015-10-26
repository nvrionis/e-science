#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Views for django rest framework .

@author: e-science Dev-team
"""
import logging
from rest_framework.generics import  GenericAPIView
from rest_framework import status
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework.permissions import AllowAny
from kamaki.clients import ClientError
from authenticate_user import *
from django.views import generic
from get_flavors_quotas import project_list_flavor_quota
from backend.models import *
from serializers import OkeanosTokenSerializer, UserInfoSerializer, \
    ClusterCreationParamsSerializer, ClusterchoicesSerializer, \
    DeleteClusterSerializer, TaskSerializer, UserThemeSerializer, \
    HdfsSerializer, StatisticsSerializer, NewsSerializer, SettingsSerializer, \
    OrkaImagesSerializer, VreImagesSerializer, DslsSerializer, DslOptionsSerializer, DslDeleteSerializer
from django_db_after_login import *
from cluster_errors_constants import *
from tasks import create_cluster_async, destroy_cluster_async, scale_cluster_async, \
    hadoop_cluster_action_async, put_hdfs_async, create_server_async, destroy_server_async, \
    create_dsl_async, import_dsl_async, destroy_dsl_async, replay_dsl_async
from create_cluster import YarnCluster
from celery.result import AsyncResult
from reroute_ssh import HdfsRequest
from okeanos_utils import check_pithos_path, check_pithos_object_exists, get_pithos_container_info
from django.db import models


logging.addLevelName(REPORT, "REPORT")
logging.addLevelName(SUMMARY, "SUMMARY")
logger = logging.getLogger("report")

logging_level = REPORT
logging.basicConfig(format='%(asctime)s:%(levelname)s:%(message)s',
                   level=logging_level, datefmt='%H:%M:%S')

class MainPageView(generic.TemplateView):
    """Load the template file"""
    template_name = 'index.html'

class SettingsView(GenericAPIView):
    """
    Instance settings.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (AllowAny, )
    resource_name = 'setting'
    serializer_class = SettingsSerializer
    
    def get(self, request, *args, **kwargs):
        settings = Setting.objects.all()
        serializer_class = SettingsSerializer(settings, many=True)
        return Response(serializer_class.data)

class VreImagesView(GenericAPIView):
    """
    VRE images metadata.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (AllowAny, )
    resource_name = 'vreimage'
    serializer_class = VreImagesSerializer

    def get(self, request, *args, **kwargs):
        """
        Return VRE image data.
        """
        image_data = VreImage.objects.all()
        serializer_class = VreImagesSerializer(image_data, many=True)
        return Response(serializer_class.data)

class OrkaImagesView(GenericAPIView):
    """
    Orka image metadata.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (AllowAny, )
    resource_name = 'orkaimage'
    serializer_class = OrkaImagesSerializer
    
    def get(self, request, *args, **kwargs):
        """
        Return orka image data.
        """
        image_data = OrkaImage.objects.all()
        serializer_class = OrkaImagesSerializer(image_data, many=True)
        return Response(serializer_class.data)

class NewsView(GenericAPIView):
    """
    News on homepage.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (AllowAny, )
    resource_name = 'newsitem'
    serializer_class = NewsSerializer

    def get(self, request, *args, **kwargs):
        """
        Return news items.
        """
        public_news = PublicNewsItem.objects.all()
        serializer_class = NewsSerializer(public_news, many=True)
        return Response(serializer_class.data)

class StatisticsView(GenericAPIView):
    """
    Statistics about history on homepage.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (AllowAny, )
    resource_name = 'statistic'
    serializer_class = StatisticsSerializer

    def get(self, request, *args, **kwargs):
        """
        Return cluster statistics for all users from database.
        """
        destroyed_clusters = ClusterInfo.objects.all().filter(cluster_status=0).count()
        active_clusters = ClusterInfo.objects.all().filter(cluster_status=1).count()
        spawned_clusters = active_clusters + destroyed_clusters
        cluster_statistics = ClusterStatistics.objects.create(spawned_clusters=spawned_clusters,
                                                             active_clusters=active_clusters)
        serializer_class = StatisticsSerializer(cluster_statistics)
        return Response(serializer_class.data)

class HdfsView(GenericAPIView):
    """
    File transfer to HDFS.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (IsAuthenticated, )
    resource_name = 'hdfs'
    serializer_class = HdfsSerializer

    def post(self, request, *args, **kwargs):
        """
        Put file in HDFS from Ftp,Http,Https or Pithos.
        """
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid():
            cluster = ClusterInfo.objects.get(id=serializer.data['id'])
            if not serializer.data['user']:
                serializer.data['user'] = ''
            if not serializer.data['password']:
                serializer.data['password'] = ''
            user_args = dict()
            user_args = serializer.data.copy()
            user_args.update({'master_IP': cluster.master_IP})
            hdfs_task = put_hdfs_async.delay(user_args)
            task_id = hdfs_task.id
            return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)
        # This will be send if user's cluster parameters are not de-serialized
        # correctly.
        return Response(serializer.errors)

class JobsView(GenericAPIView):
    """
    Info for celery tasks.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (IsAuthenticated, )
    resource_name = 'job'
    serializer_class = TaskSerializer

    def get(self, request, *args, **kwargs):
        """
        Get method for celery task state.
        """
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid():
            task_id = serializer.data['task_id']
            c_task = AsyncResult(task_id)
            user_token = Token.objects.get(key=request.auth)
            user = UserInfo.objects.get(user_id=user_token.user.user_id)

            if c_task.ready():
                if c_task.successful():
                    return Response({'success': c_task.result})
                return Response({'error': c_task.result["exc_message"]})

            else:
                return Response({'state': c_task.state})
        return Response(serializer.errors)

class StatusView(GenericAPIView):
    """
    Cluster Actions ,                                                                                          
    Create: input Object 
    project_name -- "escience.grnet.gr"
    cluster_name -- "test_cluster01"
    cluster_size -- 2
    cpu_master -- 2 
    ram_master --2048 
    disk_master --10 
    cpu_slaves -- 2
    ram_slaves -- 2048 
    disk_slaves -- 10
    disk_template -- "Standard" 
    os_choice -- "Hadoop-2.5.2" 
    ssh_key_selection -- "nick_key"
    replication_factor -- "1" 
    dfs_blocksize -- "128" 
    admin_password -- "" 
    cluster_edit -- null
    Response -- Object {id : 1 , task_id:"3be2fe85-bf8a-4a41-935b-067f0c80bfe7"}
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (IsAuthenticatedOrIsCreation, )
    resource_name = 'clusterchoice'
    serializer_class = ClusterCreationParamsSerializer

    def put(self, request, *args, **kwargs):
        """
        Handles ember requests with user's cluster creation parameters.
        Check the parameters with HadoopCluster object from create_cluster
        script.
        """
        self.serializer_class = ClusterchoicesSerializer
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid():
            user_token = Token.objects.get(key=request.auth)
            user = UserInfo.objects.get(user_id=user_token.user.user_id)
            if serializer.data['hadoop_status']:
                try:
                    cluster_action = hadoop_cluster_action_async.delay(serializer.data['id'],
                                                                       serializer.data['hadoop_status'])
                    task_id = cluster_action.id
                    return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)
                except Exception, e:
                    return Response({"status": str(e.args[0])})
            # Update existing cluster
            if serializer.data['cluster_edit']:
                cluster = ClusterInfo.objects.get(id=serializer.data['cluster_edit'])
                cluster_delta = serializer.data['cluster_size']-cluster.cluster_size
                try:
                    cluster_action = scale_cluster_async.delay(user.okeanos_token, serializer.data['cluster_edit'], cluster_delta)
                    task_id = cluster_action.id
                    return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)
                except Exception, e:
                    return Response({"status": str(e.args[0])})
            # Create cluster
            # Dictionary of YarnCluster arguments
            choices = dict()
            choices = serializer.data.copy()
            choices.update({'token': user.okeanos_token})
            try:
                YarnCluster(choices).check_user_resources()
            except ClientError, e:
                return Response({"id": 1, "message": e.message})
            except Exception, e:
                return Response({"id": 1, "message": e.args[0]})
            c_cluster = create_cluster_async.delay(choices)
            task_id = c_cluster.id
            return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)

        # This will be send if user's cluster parameters are not de-serialized
        # correctly.
        return Response(serializer.errors)

    def delete(self, request, *args, **kwargs):
        """
        Delete cluster from ~okeanos.
        """
        self.serializer_class = DeleteClusterSerializer
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid():
            user_token = Token.objects.get(key=request.auth)
            user = UserInfo.objects.get(user_id=user_token.user.user_id)
            d_cluster = destroy_cluster_async.delay(user.okeanos_token, serializer.data['id'])
            task_id = d_cluster.id
            return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)
        # This will be send if user's delete cluster parameters are not de-serialized
        # correctly.
        return Response(serializer.errors)

class ClustersView(GenericAPIView):
    """
    Cluster creation parameters. Example.
    id -- 2
    
    user_id -- 1
    
    project_name -- escience.grnet.gr
    
    vms_max --25
    
    vms_av -- Array [0:1, 1:2, 2:3, 3:4, 4:5, 5:6, 6:7, 7:8]
    
    cpu_max -- 50
    
    cpu_av -- 34
    
    ram_max -- 61440
    
    ram_av -- 45056
    
    disk_max -- 400
    
    disk_av -- 300
    
    net_av -- 6

    floatip_av -- 10
    
    cpu_choices -- Array [0:1, 1:2, 2:4, 3:8]
    
    ram_choices -- Array [0:512, 1:1024, 2:2048, 3:4096, 4:6144, 5:8192]

    disk_choices -- Array [0:5, 1:10, 2:20, 3:40, 4:60]

    disk_template -- Array [0:"Standard"]

    os_choices -- Array [0:Array[0: "Debian Base", 1: "Cloudera-CDH-5.4.4", 2: "Ecosystem-on-Hue-3.8.0", 3: "Hue-3.8.0", 4: "Hadoop-2.5.2"], 1:Array[0:"BigBlueButton-0.81", 1:"Drupal-7.37", 2:"Mediawiki-1.2.4", 3:"Redmine-3.0.4", 4:"DSpace-5.3"]]
    
    ssh_keys_names -- Array [0:nick_key]

    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (IsAuthenticatedOrIsCreation, )
    resource_name = 'clusters'
    serializer_class = ClusterCreationParamsSerializer

    
    def get(self, request, *args, **kwargs):
        """
        Return a serialized ClusterCreationParams model with information
        retrieved by kamaki calls. User with corresponding status will be
        found by the escience token.
        """
        user_token = Token.objects.get(key=request.auth)
        self.user = UserInfo.objects.get(user_id=user_token.user.user_id)
        retrieved_cluster_info = project_list_flavor_quota(self.user)
        serializer = self.serializer_class(retrieved_cluster_info, many=True)
        return Response(serializer.data)

class SessionView(GenericAPIView):
    """
    User login and logout and
    user theme update
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (IsAuthenticatedOrIsCreation, )
    resource_name = 'user'
    serializer_class = OkeanosTokenSerializer
    user = None

    def get(self, request, *args, **kwargs):
        """
        Return a UserInfo object from db.
        User will be found by the escience token.
        """
        user_token = Token.objects.get(key=request.auth)
        self.user = UserInfo.objects.get(user_id=user_token.user.user_id)
        self.serializer_class = UserInfoSerializer(self.user)
        return Response(self.serializer_class.data)

    def post(self, request, *args, **kwargs):
        """
        Authenticate a user with a ~okeanos token.  Return
        appropriate success flag, user id, cluster number
        and escience token or appropriate  error messages in case of
        error.
        """
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid():
            token = serializer.data['token']
            if check_user_credentials(token) == AUTHENTICATED and check_user_uuid(token) == 0:
                self.user = db_after_login(token)
                self.serializer_class = UserInfoSerializer(self.user)
                return Response(self.serializer_class.data)
            else:
                return Response(status=status.HTTP_401_UNAUTHORIZED)
        else:
            return Response(serializer.errors,
                            status=status.HTTP_400_BAD_REQUEST)

    def put(self, request, *args, **kwargs):
        """
        Updates user status in database on user logout or user theme change.
        """
        user_token = Token.objects.get(key=request.auth)
        self.user = UserInfo.objects.get(user_id=user_token.user.user_id)
        self.serializer_class = UserThemeSerializer
        serializer = self.serializer_class(data=request.DATA)
        if serializer.is_valid():
            if serializer.data['user_theme']:
                self.user.user_theme = serializer.data['user_theme']
                self.user.save()
            else:
                db_logout_entry(self.user)

            self.serializer_class = UserInfoSerializer(self.user)
            return Response(self.serializer_class.data)

        else:
            return Response(serializer.errors,
                            status=status.HTTP_400_BAD_REQUEST)
            
class VreServerView(GenericAPIView):
    """
    Virtual Research Environment servers creation, deletion and info.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (IsAuthenticated, )
    resource_name = 'vreserver'
    model = VreServer
    
    def get(self, request, *args, **kwargs):
        """
        Return a serialized Vreserver model with information
        retrieved by kamaki calls. User with corresponding status will be
        found by the escience token.
        """
        user_token = Token.objects.get(key=request.auth)
        self.user = UserInfo.objects.get(user_id=user_token.user.user_id)
        retrieved_server_info = project_list_flavor_quota(self.user)
        serializer = ClusterCreationParamsSerializer(retrieved_server_info, many=True)
        return Response(serializer.data)
    
    def post(self, request, *args, **kwargs):
        """
        Handles requests with user's VRE server creation parameters.
        """
        serializer_class = ClusterchoicesSerializer
        serializer = serializer_class(data=request.DATA)
        if serializer.is_valid():
            user_token = Token.objects.get(key=request.auth)
            user = UserInfo.objects.get(user_id=user_token.user.user_id)

            # Dictionary of VreServer arguments
            choices = dict()
            choices = serializer.data.copy()
            choices.update({'token': user.okeanos_token, 'cluster_size': 1,"cpu_slaves": 0,"ram_slaves": 0,
                            "disk_slaves": 0,"cpu_master": choices['cpu'],"ram_master": choices['ram'],
                            "disk_master": choices['disk']})
            c_server = create_server_async.delay(choices)
            task_id = c_server.id
            return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)

        # This will be send if user's parameters are not de-serialized
        # correctly.
        return Response(serializer.errors)
    
    def delete(self, request, *args, **kwargs):
        """
        Delete VRE server from ~okeanos.
        """ 
        serializer = DeleteClusterSerializer(data=request.DATA)
        if serializer.is_valid():
            user_token = Token.objects.get(key=request.auth)
            user = UserInfo.objects.get(user_id=user_token.user.user_id)
            d_server = destroy_server_async.delay(user.okeanos_token, serializer.data['id'])
            task_id = d_server.id
            return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)
        # This will be send if user's delete server parameters are not de-serialized
        # correctly.
        return Response(serializer.errors)
    
class DslView(GenericAPIView):
    """
    User DSL management.
    """
    authentication_classes = (EscienceTokenAuthentication, )
    permission_classes = (IsAuthenticated, )
    resource_name = 'dsl'
    model = Dsl
    
    def post(self, request, *args, **kwargs):
        """
        Handles requests with user's Reproducible Experiments metadata file creation parameters.
        """
        serializer = DslOptionsSerializer(data=request.DATA)
        if serializer.is_valid():
            user_token = Token.objects.get(key=request.auth)
            user = UserInfo.objects.get(user_id=user_token.user.user_id)

            # Dictionary of UserDSL arguments
            choices = dict()
            choices = serializer.data.copy()
            choices.update({'token': user.okeanos_token})
            choices['pithos_path'] = check_pithos_path(choices['pithos_path'])
            choices.update({'pithos_path': choices['pithos_path']})
            uuid = get_user_id(unmask_token(encrypt_key, choices['token']))
            if serializer.data['cluster_id'] == -1:
                choices.update({'cluster_id': None})
                dsl_file_status_code = check_pithos_object_exists(choices['pithos_path'], choices['dsl_name'], choices['token'])
                if dsl_file_status_code == pithos_object_not_found:
                    return Response(serializer.errors,
                            status=status.HTTP_404_NOT_FOUND)
                i_dsl = import_dsl_async.delay(choices)
                task_id = i_dsl.id
                return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)
            container_status_code = get_pithos_container_info(choices['pithos_path'], choices['token'])
            if container_status_code == pithos_container_not_found:
                return Response(serializer.errors,
                            status=status.HTTP_404_NOT_FOUND)
            c_dsl = create_dsl_async.delay(choices)
            task_id = c_dsl.id
            return Response({"id":1, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)

        # This will be send if user's parameters are not de-serialized
        # correctly.
        return Response(serializer.errors)
    
    def get(self, request, *args, **kwargs):
        """
        Return a serialized Cluster metadata model. User with corresponding status will be
        found by the escience token.
        """
        user_token = Token.objects.get(key=request.auth)
        self.user = UserInfo.objects.get(user_id=user_token.user.user_id)
        serializer_class = DslsSerializer
        serializer = self.serializer_class(data=request.DATA, many=True)
        return Response(serializer.data)
    
    def put(self, request, *args, **kwargs):
        """
        Use the experiment metadata to replay an experiment. Create cluster if necessary, then perform the actions.
        """
        serializer = DslDeleteSerializer(data=request.DATA)
        if serializer.is_valid():
            user_token = Token.objects.get(key=request.auth)
            user = UserInfo.objects.get(user_id=user_token.user.user_id)
            dsl_id = serializer.data['id']
            r_dsl = replay_dsl_async.delay(user.okeanos_token, dsl_id)
            task_id = r_dsl.id
            return Response({"id":dsl_id, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)
        return Response(serializer.errors)  
    
    def delete(self, request, *args, **kwargs):
        """
        Delete Reproducible Experiments metadata file from pithos.
        """ 
        serializer = DslDeleteSerializer(data=request.DATA)
        if serializer.is_valid():
            user_token = Token.objects.get(key=request.auth)
            user = UserInfo.objects.get(user_id=user_token.user.user_id)
            dsl_id = serializer.data['id']
            d_dsl = destroy_dsl_async.delay(user.okeanos_token, dsl_id)
            task_id = d_dsl.id
            return Response({"id":dsl_id, "task_id": task_id}, status=status.HTTP_202_ACCEPTED)
        # This will be send if user's delete server parameters are not de-serialized
        # correctly.
        return Response(serializer.errors)    