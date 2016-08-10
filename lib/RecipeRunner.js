const fs      = require('fs')
  , snake   = require('snake-case')
  , keys    = require('map-tabular-keys')
  , coerce  = require('coerce-tabular')
  , tabular = require('tabular-stream')
  , excel   = require('excel-stream')
  , PassThrough = require('stream').PassThrough;

const StreamTransform = require('jsonpath-object-transform').Stream;

const excel_sheet = function(sheet_index,keyfunc) {
  return excel({sheetIndex: sheet_index}).pipe(keys(keyfunc)).pipe(coerce());
};

const helper_functions = {
  extract_site : function(site) { return [ parseInt(site.split(':')[1].substring(1)), "HexNAc"]; },
  clean : function(string) { return string.replace(/[\(\)\s\:]+/g,''); },
  lookup : function(search,table) { return table[search]; }
};

const RecipeRunner = function(recipe) {
	let read_streams = recipe.sources.files.map(function(file) {
		let file_index = file.split(':');
		if (file_index.length > 1) {
			return this.readFile(file_index[0]).pipe( excel_sheet(parseInt(file_index[1]),snake) );
		} else {
			return this.readFile(file_index[0]).pipe( tabular(snake) );
		}
	});
	return read_streams[0].pipe().pipe(this.writeFile());
};

RecipeRunner.prototype.transform = function(recipe) {
  return StreamTransform(recipe.template, 'data',{
    functions: helper_functions,
    environment : recipe.environment
  });
};

RecipeRunner.prototype.readFile = function(filename) {
  return fs.createReadStream(filename);
};

RecipeRunner.prototype.writeFile = function() {
  return new PassThrough({objectMode: true});
};

module.exports = RecipeRunner;