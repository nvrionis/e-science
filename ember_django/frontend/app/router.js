// Main application routes
App.Router.map(function() {
	this.route('homepage');
	this.resource('user', function() {
		// /user/login
		this.route('login');
		// /user/logout
		this.route('logout');
		// /user/welcome
		this.route('welcome');
	});
	this.resource('cluster', function() {
		// /cluster/create
		this.route('create');		
		// cluster management route
		// /cluster/cluster_id
		this.route('management', { path: "/:usercluster.id" });
	});
	this.resource('help', function() {
		// /help/images
		this.route('images');
	});
	// Route to enforce login policy
	// other routes that require login extend this
	this.route('restricted');
});
