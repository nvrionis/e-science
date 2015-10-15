attr = App.attr;
// Model used for retrieving cluster creation information 
// based on user's quota and kamaki flavors
App.Cluster = DS.Model.extend({
	project_name : attr(),		    // name of the project
	vms_max : attr('number'),    	// maximum (limit) number of VMs
	vms_av : attr(),             	// available VMs
	cpu_max : attr('number'),    	// maximum CPUs
	cpu_av : attr('number'),     	// available CPUs
	ram_max : attr('number'),    	// maximum ram
	ram_av : attr('number'),     	// available ram
	disk_max : attr('number'),  	// maximum disk space
	disk_av : attr('number'),    	// available disk space
	net_av : attr(),                // available networks
	floatip_av : attr(),            // available floating ips	
	cpu_choices : attr(),        	// CPU choices
	ram_choices : attr(),        	// ram choices
	disk_choices : attr(),       	// disk choices
	disk_template : attr(),      	// storage choices
	os_choices : attr(),          	// Operating System choices
	hadoop_choices : function(){
	    return this.get('os_choices')[0];
	}.property('os_choices'),       // Filter for Hadoop Images
    project_name_clean : function(){
        var project_name = this.get('project_name');
        var numchar = project_name.lastIndexOf(":");
        return numchar == -1 ? project_name : project_name.slice(0,numchar);
    }.property('project_name'),     // Remove guid from system project name
    project_name_decorated : function(){
        var name = this.get('project_name_clean');
        var template_bs3 = '<span class="col col-sm-3 text-left pull-left">%@ </span>'+
                           '<span class="col col-sm-2 text-right pull-left">VM:%@</span>'+
                           '<span class="col col-sm-2 text-right pull-left">CPU:%@</span>'+
                           '<span class="col col-sm-3 text-right pull-left">RAM:%@MB</span>'+
                           '<span class="col col-sm-2 text-right pull-left">Disk:%@GB</span>';
        var decorated_name = template_bs3.fmt(name,this.get('vms_av').get('lastObject') || 0,this.get('cpu_av'),this.get('ram_av'),this.get('disk_av'));
        return Ember.String.htmlSafe(decorated_name);
    }.property('project_name_clean'), // decorate project name with project resource info in columns	
	vm_flavors_choices : ['Small', 'Medium', 'Large'],  //Predefined VM Flavors
	ssh_keys_names : attr()         // ssh key's names
});

// For Fixtures
/*App.Cluster.reopenClass({
FIXTURES: [
    {
	id: 1,
	project_name : 'system',
	vms_max : 16,
	vms_av : [2,4,8,16],
	cpu_max : 32,
	cpu_av : 32,
	ram_max : 2048,
	ram_av : 2048,
	disk_max : 200,
	disk_av : 100,
	cpu_choices : [1,2,4,8],
	ram_choices : [128, 256,512],
	disk_choices : [20,40,60,80,100],
	disk_template : ['disk1','disk2'],
	os_choices : ['os1','os2']
    },
    {
	id: 2,
	project_name : 'escience.grnet.gr',
	vms_max : 32,
	vms_av : [8,16],
	cpu_max : 32,
	cpu_av : 16,
	ram_max : 2048,
	ram_av : 1024,
	disk_max : 400,
	disk_av : 200,
	cpu_choices : [4,8],
	ram_choices : [256,512],
	disk_choices : [50,100],
	disk_template : ['disk3','disk4'],
	os_choices : ['os3','os4']
    }
]
});*/
