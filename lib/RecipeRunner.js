'use strict';

const fs   = require('fs');

const StreamTransform = require('jsonpath-object-transform').Stream;

const helper_functions = {
  match : function(string,pattern,index) {
    var re = new RegExp(pattern, 'g');
    var val;
    var vals = [];
    while ( (val = re.exec(string)) !== null) {
      vals.push(val[1]);
    }
    vals = vals.map(function(match) { return {'value' : match}; });
    return (! index && index !== 0) ? vals : (vals[index] || {}).value;
  },
  paste : function() { return Array.prototype.slice.call(arguments).join(''); },
  array : function() { return Array.prototype.slice.call(arguments); },
  clean : function(string) { return string.replace(/[^A-Z0-9\-\_]+/g,''); },
  lookup : function(search,table) { return table[search]; }
};

const RecipeRunner = function(recipe) {
  return this.transform(recipe);
};

RecipeRunner.prototype.transform = function(recipe) {
  return StreamTransform(recipe.template, 'data',Object.assign({}, helper_functions, recipe.environment));
};

RecipeRunner.prototype.readFile = function(filename) {
  return fs.createReadStream(filename);
};

module.exports = RecipeRunner;