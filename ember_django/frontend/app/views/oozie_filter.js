//Button View to enable and disable oozie filter option 
App.OozieFilterButView  = Ember.View.extend({
    tagName: 'button',
    // class names, :emberbutton for CSS style
    classNameBindings: [':emberbutton'],
    // html attributes, custom (e.g. name, value) should be defined here
    attributeBindings: ['name', 'value'],
    // initialization
    init: function() {
        // set id
	    this.set('elementId', "oozie_filter_" + this.get('value'));
	    return this._super();
	},        
    // on click    
    click: function () {
	// for this controller, trigger the ram_selection and send the value
        this.get('controller').send('oozie_filter_selection', this.get('value'), this.get('name'));
    }
});