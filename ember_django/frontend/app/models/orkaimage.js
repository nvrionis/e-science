attr = App.attr;
// Model used for retrieving OrkaImage information from backend DB
App.Orkaimage = DS.Model.extend({
	image_name : attr('string'), // OrkaImage name
	image_pithos_uuid : attr('string'), // Linked Pithos Image UUID
	image_components : attr('string'), // Stringified OrkaImage Components metadata (json.dumps)

	components : function() {
		var compobj = JSON.parse(this.get('image_components'));
		var comp = [];
		for (key in compobj) {
			comp.push([key, compobj[key]]);
		}
		return comp;
	}.property('image_components'),

	active_image : function() {
		var name = this.get('image_name');
		if (name == 'Cloudera-CDH-5.4.2') {
			return true;
		} else {
			return false;
		}
	}.property('image_name'),

	image_href : function() {
		var uuid = this.get('image_pithos_uuid');
			return '#'+uuid;
	}.property('image_pithos_uuid')
});
