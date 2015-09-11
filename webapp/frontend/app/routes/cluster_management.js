App.ClusterManagementRoute = App.RestrictedRoute.extend({

	// model for cluster management route
	needs: ['clusterManagement','userWelcome'],
	model: function(params, transition) {

		var that = this;
		// find the correct cluster
		var promise = this.store.fetch('user', 1).then(function(user) {
	
			// find all clusters of user
			var clusters = user.get('clusters');
			var length = clusters.get('length');
			if (length > 0) {
			    var bPending = false;
				for (var i = 0; i < length; i++) {
					// check for the cluster id
					if (clusters.objectAt(i).get('id') == params["usercluster.id"])
					{
					    var this_cluster = clusters.objectAt(i);
						that.controllerFor('clusterManagement').send('help_hue_login', this_cluster.get('os_image'));
                        if ((this_cluster.get('cluster_status') == '2') || (this_cluster.get('hadoop_status') == '2')) {
                            that.controllerFor('clusterManagement').send('timer', true, that.store);
                            bPending = true;
                        }else{
                            that.controllerFor('clusterManagement').send('timer',false);
                        }
					 	return this_cluster;
					}
				}
				if (!bPending){
				    that.controllerFor('clusterManagement').send('timer',false);
				}
			}
	
 		}, function(reason) {
			console.log(reason.message);
			that.controllerFor('clusterManagement').send('timer',false);
		});
	 	return promise;
	},
	setupController : function(controller, model){
	    this._super(controller,model);
	    controller.set('slaves_increment_loader',true);
        var store = this.get('store');
        var project_name = model.get('project_name');
        var promise = store.find('cluster');
        promise.then(function(data){
            var arr_projects = data.get('content');
            for (i=0;i<arr_projects.length;i++){
                if (arr_projects[i].get('project_name') == project_name){
                    controller.set('cluster_project_data',arr_projects[i]);
                    controller.set('slaves_increment_loader',false);
                    break;
                }
            }
        },function(reason){
            controller.set('slaves_increment_loader',false);
            console.log(reason.message);
        });	    
	},
	
	// possible actions
	actions: {
	    willTransition : function(transition) {
            // leaving this route
            this.controller.send('timer', false);
            this.controller.send('setActiveTab','info');
        },
		
		takeClusterAction : function(cluster) {
			var self = this;
			var store = this.store;
			var action = cluster.get('cluster_confirm_action');
			cluster.set('cluster_confirm_action', false);
			switch(action) {
			case 'cluster_delete':
				cluster.destroyRecord().then(function(data) {
					var count = self.controller.get('count');
					var extend = Math.max(5, count);
					self.controller.set('count', extend);
					self.controller.send('timer', true, store);
				}, function(reason) {
					console.log(reason.message);
					if (!Ember.isBlank(reason.message)){
						var msg = {'msg_type':'danger','msg_text':reason.message};
                        self.controllerFor('userWelcome').send('addMessage',msg);
					}
				});
				break;
			case 'hadoop_start':
				cluster.set('hadoop_status','start');
				cluster.save().then(function(data){
					var count = self.controller.get('count');
					var extend = Math.max(5, count);
					self.controller.set('count', extend);
					self.controller.send('timer', true, store);
				},function(reason){
					console.log(reason.message);
					if (!Ember.isBlank(reason.message)){
						var msg = {'msg_type':'danger','msg_text':reason.message};
                        self.controllerFor('userWelcome').send('addMessage',msg);
					}
				});
				break;
			case 'hadoop_stop':
				cluster.set('hadoop_status','stop');
				cluster.save().then(function(data){
					var count = self.controller.get('count');
					var extend = Math.max(5, count);
					self.controller.set('count', extend);
					self.controller.send('timer', true, store);
				},function(reason){
					console.log(reason.message);
					if (!Ember.isBlank(reason.message)){
						var msg = {'msg_type':'danger','msg_text':reason.message};
                        self.controllerFor('userWelcome').send('addMessage',msg);
					}
				});
				break;
			case 'hadoop_format':
				cluster.set('hadoop_status','format');
				cluster.save().then(function(data){
					var count = self.controller.get('count');
					var extend = Math.max(5, count);
					self.controller.set('count', extend);
					self.controller.send('timer', true, store);
				},function(reason){
					console.log(reason.message);
					if (!Ember.isBlank(reason.message)){
						var msg = {'msg_type':'danger','msg_text':reason.message};
                        self.controllerFor('userWelcome').send('addMessage',msg);
					}
				});
				break;
			}
		},
		
		confirmClusterAction : function(cluster, value) {
			cluster.set('cluster_confirm_action', value);
		}	
	},
	deactivate : function() {
        // left this route
        this.controller.send('timer', false);
    }
	  
});