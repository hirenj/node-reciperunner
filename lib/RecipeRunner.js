'use strict';

const StreamTransform = require('jsonpath-object-transform').Stream;

const helper_functions = {
  match : function(string,pattern,index) {
    var re = new RegExp(pattern, 'g');
    var val;
    var vals = [];
    while ( (val = re.exec(string)) !== null) {
      if (val.length <= 2) {
        vals.push(val[1]);
      } else {
        vals.push(val.splice(1));
      }
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

module.exports = RecipeRunner;