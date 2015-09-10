var app = angular.module('books', []);

app.controller("BooksCtrl", function($scope, $http) {

    $scope.books = [];
    $scope.newBook = {};
    
    $scope.load = function() {
	$http.get("/api/lib/book")
            .success(function(data) {
		$scope.books = data;
            });
    }

    $scope.delete = function(book) {
	$http.delete("/api/lib/book/"+book['@id'])
	    .success(function() {
		$scope.load();
	    });	
    }

    $scope.create = function() {
	$http.post("/api/lib/book", $scope.newBook)
            .success(function(data) {
		$scope.load();
		$scope.newBook = {};
            });
    }

    $scope.load();
});
