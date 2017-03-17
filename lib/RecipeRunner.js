'use strict';

const StreamTransform = require('jsonpath-object-transform').Stream;

const PassThrough = require('stream').PassThrough;

const StreamCombiner = function() {
  let self = this;
  this.streams = Array.prototype.slice.apply(arguments);
  this.transformStream = new PassThrough({objectMode: false});
  let source = null;
  self.streams.forEach((stream,idx) => source = source ? source.pipe(self.streams[idx]) : self.streams[idx] );
  source.pipe(self.transformStream);

  this.transformStream.on('pipe', function(source) {
    source.unpipe(this);
    source.pipe(self.streams[0]);
  });

  return this.transformStream;
};

const is_numeric = function (val) {
    return Number(parseFloat(val))==val;
};

const add_value = function(val,addition) {
  if (Array.isArray(val)) {
    return val.map( a_val => add_value(a_val,addition) );
  }
  if (is_numeric(val)) {
    return +val + addition;
  }
  return val;
};

const helper_functions = {
  repeat: function(value,count) {
    return Array.apply(null,Array(count)).map( (val,idx) => { return { value: value, index: idx }; } );
  },
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
  sum : function(value,addition) {
    if (Array.isArray(value)) {
      return value.map( (val) => {
        if (typeof val == 'object' && val.value) {
          val.value = add_value(val.value,addition);
          return val;
        } else {
          return add_value(value,addition);
        }
      });
    } else {
      return add_value(value,addition);
    }
  },
  lastvalue : function(value) {
    var new_value = value ? value : helper_functions.lastvalue.value;
    helper_functions.lastvalue.value = new_value;
    return new_value;
  },
  Math: Math,
  paste : function() { return Array.prototype.slice.call(arguments).join(''); },
  array : function() { return Array.prototype.slice.call(arguments); },
  clean : function(string, chars) {
    if (! chars) {
      chars = 'A-Za-z0-9\-\_';
    }
    return string ? string.toString().replace(new RegExp('[^'+chars+']+','g'),'') : string;
  },
  toupper: function(val) { return (val || '').toString().toUpperCase(); },
  lookup : function(search,table) { return table[search]; }
};

const RecipeRunner = function(recipe) {
  return this.transform(recipe);
};

RecipeRunner.prototype.transform = function(recipe) {
  let preprocess = null;
  if (recipe.preprocess) {
    preprocess = (StreamTransform(recipe.preprocess,'data',Object.assign({}, helper_functions, recipe.environment)));
    preprocess._readableState.objectMode = false;
  }
  let output = StreamTransform(recipe.template, 'data',Object.assign({}, helper_functions, recipe.environment));
  if (preprocess) {
    return (new StreamCombiner(preprocess,output));
  } else {
    return output;
  }

};

module.exports = RecipeRunner;