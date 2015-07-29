attr = App.attr;
// Model used for retrieving VRE server creation information 
// based on user's quota and kamaki flavors
App.Vreserver = DS.Model.extend({
	project_name : attr(),		    // name of the project
	vms_max : attr('number'),    	// maximum (limit) number of VMs
	vms_av : attr(),             	// available VMs
	cpu_max : attr('number'),    	// maximum CPUs
	cpu_av : attr('number'),     	// available CPUs
	ram_max : attr('number'),    	// maximum ram
	ram_av : attr('number'),     	// available ram
	disk_max : attr('number'),  	// maximum disk space
	disk_av : attr('number'),    	// available disk space
	floatip_av : attr(),            // available floating ips	
	cpu_choices : attr(),        	// CPU choices
	ram_choices : attr(),        	// ram choices
	disk_choices : attr(),       	// disk choices
	disk_template : attr(),      	// storage choices
	os_choices : attr(),          	// Operating System choices
	vm_flavors_choices : ['Small', 'Medium', 'Large'],  //Predefined VM Flavors
	ssh_keys_names : attr()         // ssh key's names
});

