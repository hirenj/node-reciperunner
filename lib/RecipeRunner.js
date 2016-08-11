"use strict";

const fs      = require('fs')
  , snake   = require('snake-case')
  , keys    = require('map-tabular-keys')
  , coerce  = require('coerce-tabular')
  , tabular = require('tabular-stream')
  , excel   = require('excel-stream')
  , path    = require('path')
  , PassThrough = require('stream').PassThrough;

const StreamTransform = require('jsonpath-object-transform').Stream;

const excel_sheet = function(input,sheet_index,keyfunc) {
  return input.pipe(excel({sheetIndex: sheet_index})).pipe(keys(keyfunc)).pipe(coerce());
};

const helper_functions = {
  match : function(string,pattern,index,coerce) {
    var val = (string.match(new RegExp(pattern, index !== null ? 'g' : null))||[])[index || 0];
    if (coerce && val) {
      return parseInt(val);
    } else {
      return val;
    }
  },
  paste : function() { return Array.prototype.slice.call(arguments).join(''); },
  array : function() { return Array.prototype.slice.call(arguments) },
  clean : function(string) { return string.replace(/[^A-Z0-9\-\_]+/g,''); },
  lookup : function(search,table) { return table[search]; }
};

const RecipeRunner = function(recipe,base) {
  var self = this;
  let read_streams = recipe.sources.files.map(function(file) {
    let file_index = file.split(':');
    if (file_index.length > 1) {
      return excel_sheet( self.readFile(path.join(base,file_index[0])), parseInt(file_index[1]),snake);
    } else {
      return self.readFile(path.join(base,file_index[0])).pipe( tabular(snake) );
    }
  });
  var joiner = new PassThrough({objectMode : true});
  read_streams.forEach( (stream) => stream.pipe(joiner));
  return joiner.pipe(this.transform(recipe)).pipe(this.writeFile());
};

RecipeRunner.prototype.transform = function(recipe) {
  return StreamTransform(recipe.template, 'data',Object.assign({}, helper_functions, recipe.environment));
};

RecipeRunner.prototype.readFile = function(filename) {
  return fs.createReadStream(filename);
};

RecipeRunner.prototype.writeFile = function() {
  return new PassThrough({objectMode: true});
};

module.exports = RecipeRunner;