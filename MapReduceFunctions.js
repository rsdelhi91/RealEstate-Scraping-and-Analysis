/*
* Javascript functions for MapReduce on CouchDB futon to analyse the stored
* data in the view.
*
* Author: Rahul Sharma
* Email: sharma1@student.unimelb.edu.au
*
*/


/*
* provide a count of all the postings where a price has been provided and the
* locality is specified, where the property is available for rent.
*
*/

// Map function
function(doc) {
 if((parseInt(doc.postcode) >= 3000) && (parseInt(doc.postcode) <= 3996)){
   if(doc.locality != "N/A" && doc.price != "N/A" && doc.tag == "rent"){
     emit(doc.locality, 1);
   }
 }
}

// Reduce function
function(k,v){
   return sum(v)
}


/*
* provide a count of all the property postings available for buying based on
* postcode. This will be used to identify the top 10 areas for available properties
* for buying.
*
*/

// Map function
function(doc) {
   if((doc.tag == "buy") && (doc.postcode != "N/A")){
	    if((parseInt(doc.postcode) >= 3000) && (parseInt(doc.postcode) <= 3996)){
        emit(doc.postcode,1);
      }
   }
}

// Reduce function
function(k,v){
   return sum(v);
}


/*
* provide a count of all the property postings available for renting based on
* postcode. This will be used to identify the top 10 areas for available properties
* for renting.
*
*/

// Map function
function(doc) {
   if((doc.tag == "rent") && (doc.postcode != "N/A")){
	    if((parseInt(doc.postcode) >= 3000) && (parseInt(doc.postcode) <= 3996)){
        emit(doc.postcode,1);
      }
   }
}

// Reduce function
function(k,v){
   return sum(v);
}

/*
* provide only those property posting documents which are available for buying
* and have a price associated with it, from the area having the postcode 3000.
*
*/

// Map function
function(doc) {
 if((doc.price != "N/A") && (doc.tag == "buy") ){
   if(parseInt(doc.postcode) == 3000){
      emit(doc,1);
   }
 }
}
