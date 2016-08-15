'use strict';

const AWS = require('lambda-helpers').AWS;
const util = require('util');
const path = require('path');

const RecipeFolder = require('./RecipeFolder');

const s3 = AWS.S3();

const RecipeFolderS3 = function(input_path,options) {
  this.bucket = options.bucket;
  this.output_prefix = options.output;
  RecipeFolder.call(this, input_path,options);
};

util.inherits(RecipeFolderS3,RecipeFolder);

RecipeFolderS3.prototype.findRecipes = function(input_path) {
  let params = {
    Bucket: this.bucket,
    Prefix: input_path
  };
  return s3.listObjects(params).promise().then(function(keys) {
    return keys.Contents.map((entry) => entry.Key );
  });
};

RecipeFolderS3.prototype.readRecipe = function(input_path) {
  let params = {
    Bucket: this.bucket,
    Key: input_path
  };
  let request = s3.getObject(params);
  return request.promise().then(function (dat) {
    let recipe = JSON.parse(dat);
    recipe.base = path.dirname(input_path);
    recipe.filename = path.basename(input_path);
    recipe.md5 = request.response.data.ETag;
    return recipe;
  });
};

RecipeFolderS3.prototype.writeRecipe = function(stream,recipe) {
  let base = recipe.base;
  let filename = recipe.filename;
  let checksum = recipe.checksum;
  let params = {
    Bucket: this.bucket,
    Key: path.join( this.output_prefix, path.basename(base)+'_'+filename),
    Body: stream,
    Metadata: {
      'recipe_checksum': checksum
    }
  };
  return s3.putObject(params).promise().then(function() {
    console.log('Uploaded',params.Key);
  });
};

let head_promises = {};

let get_md5 = function(bucket,key,checksum) {
  let params = {
    Bucket: bucket,
    Key: key
  };
  if (head_promises[bucket+'/'+key]) {
    return head_promises[bucket+'/'+key];
  }
  let request = s3.headObject(params);
  head_promises[bucket+'/'+key] = request.promise().then(function (dat) {
    return checksum ? dat.Metadata.recipe_checksum : dat.ETag;
  });
  return head_promises[bucket+'/'+key];
};

let get_checksum = function(bucket,key) {
  return get_md5(bucket,key,true);
};

RecipeFolderS3.prototype.needsUpdate = function(recipe) {
  let recipe_checksum = recipe.md5;
  let source_checksums = recipe.sources.sort().map(function(source) {
      let abs_path = path.join(recipe.base,source);
      return get_md5(this.bucket, abs_path);
  });
  return source_checksums
        .then( (md5s) => [recipe_checksum].concat(md5s).join('#') )
        .then( (checksum) => recipe.checksum = checksum )
        .then( () => get_checksum(this.bucket, path.join( this.output_prefix, path.basename(recipe.base)+'_'+recipe.filename)) )
        .then( (cached) => recipe.needs_update = (cached !== recipe.checksum));
};

RecipeFolderS3.prototype.readFile = function(filename) {
  var params = {
    'Key' : filename,
    'Bucket' : this.bucket
  };
  var request = s3.getObject(params);
  var stream = request.createReadStream();
  return stream;
};
