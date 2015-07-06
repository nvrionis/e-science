App.HomepageRoute = Ember.Route.extend({

    // model for homepage route (Cluster Statistics)
    model : function(params) {
        var that = this;
        // Perform GET request for cluster statistics
        this.store.find('homepage', 1).then(function(homepage) {
            
            that.controller.set('spawned_clusters', homepage.get('spawned_clusters'));
            that.controller.set('active_clusters', homepage.get('active_clusters'));
            return homepage;
        }, function(reason) {
            console.log(reason.message);
        });
    }
}); 