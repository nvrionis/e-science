// Help Images route
App.HelpImagesRoute = Ember.Route.extend({
	model : function() {
		var available_images = [];
		var available_keys = [];
		var available_values = [];
		var components = [];
		var that = this;
		this.store.fetch('orkaimage', {}).then(function(data) {
			for (var i = 0; i < data.get('content').length; i++) {
				available_images[i] = data.get('content').objectAt(i);//.get('image_name');
			}
			that.controller.set('images', available_images);

		}, function(reason) {
			console.log(reason.message);
		});
	},
	actions : {
		didTransition : function(transition) {

		}
	}

});
