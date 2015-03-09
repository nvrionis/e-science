// Cluster Create route
App.ClusterCreateRoute = App.RestrictedRoute.extend({
	needs : 'clusterCreate',
	// model for create cluster choices (input form)
	model : function() {
		$.loader.close(true);
		return this.store.find('cluster');
	},
	actions: {
		error: function(err) {
			// to catch errors
			// for example 401 responses
			this.transitionTo('user.logout');
    	},
    	didTransition: function(transition) {
			// resets variables every time you go to the create cluster
			this.controllerFor('clusterCreate').reset_variables();
			this.controllerFor('clusterCreate').reset_project();
			// last cluster config
			this.controllerFor('clusterCreate').set('last_cluster_conf_checked', false);
			this.controllerFor('clusterCreate').send('findLastCluster'); 
		
    	}
	}
});
