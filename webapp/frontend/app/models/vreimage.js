attr = App.attr;
// Model used for retrieving VreImage information from backend DB
App.Vreimage = DS.Model.extend({
    image_name : attr('string'), // VreImage name
    image_pithos_uuid : attr('string'), // Linked Pithos Image UUID
    image_components : attr('string'), // Stringified VreImage Components metadata (json.dumps)
    image_min_reqs : attr('string'), // stringified VreImage minimum requirements metadaga (json.dumps)
    image_faq_links : attr('string'), // stringified VreImage FAQ links (json.dumps)
    image_init_extra : attr(), // array of extra creation fields
    image_access_url : attr(), // array of access URLs
    image_category : attr('string'), // VreImageCategory name (resolving from id at backend)
    image_href : function() {
        var uuid = this.get('image_pithos_uuid');
        return '#' + uuid;
    }.property('image_pithos_uuid')
});
