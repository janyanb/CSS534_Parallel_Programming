#include <iostream>  // cout
#include <fstream>   // ifstream
#include <string.h>  // strncpy
#include <stdlib.h>  // rand
#include <math.h>    // sqrt, pow
#include <omp.h>     // OpenMP
#include "Timer.h"
#include "Trip.h"

using namespace std;

// Already implemented. see the actual implementations below
void initialize( Trip trip[CHROMOSOMES], int coordinates[CITIES][2] );
void select( Trip trip[CHROMOSOMES], Trip parents[TOP_X] );
void populate( Trip trip[CHROMOSOMES], Trip offsprings[TOP_X] );

// need to implement for your program 1
extern void evaluate( Trip trip[CHROMOSOMES], double distanceBtwCities[CITIES][CITIES] );
extern void crossover( Trip parents[TOP_X], Trip offsprings[TOP_X], double distanceBtwCities[CITIES][CITIES] );
extern void mutate( Trip offsprings[TOP_X] );
extern void calculateDistance( int coordinates[CITIES][2], double distanceBtwCities[CITIES][CITIES]);

/*
 * MAIN: usage: Tsp #threads
 */
int main( int argc, char* argv[] ) {
  Trip trip[CHROMOSOMES];       // all 50000 different trips (or chromosomes)
  Trip shortest;                // the shortest path so far
  int coordinates[CITIES][2];   // (x, y) coordinates of all 36 cities:
  int nThreads = 1;
  double distanceBtwCities[CITIES][CITIES];  

  // verify the arguments
  if ( argc == 2 )
    nThreads = atoi( argv[1] );
  else {
    cout << "usage: Tsp #threads" << endl;
    if ( argc != 1 )
      return -1; // wrong arguments
  }
  cout << "# threads = " << nThreads << endl;

  // shortest path not yet initialized
  shortest.itinerary[CITIES] = 0;  // null path
  shortest.fitness = -1.0;         // invalid distance

  // initialize 5000 trips and 36 cities' coordinates
  initialize( trip, coordinates );

  //calculates the distance between cities
  calculateDistance(coordinates, distanceBtwCities);

  // start a timer 
  Timer timer;
  timer.start( );

  // change # of threads
  omp_set_num_threads( nThreads );

  // find the shortest path in each generation
  for ( int generation = 0; generation < MAX_GENERATION; generation++ ) {

    // evaluate the distance of all 50000 trips
    evaluate( trip, distanceBtwCities );

    // just print out the progress
    if ( generation % 20 == 0 )
      cout << "generation: " << generation << endl;

    // whenever a shorter path was found, update the shortest path
    if ( shortest.fitness < 0 || shortest.fitness > trip[0].fitness ) {

      strncpy( shortest.itinerary, trip[0].itinerary, CITIES );
      shortest.fitness = trip[0].fitness;

      cout << "generation: " << generation 
	   << " shortest distance = " << shortest.fitness
	   << "\t itinerary = " << shortest.itinerary << endl;
    }

    // define TOP_X parents and offsprings.
    Trip parents[TOP_X], offsprings[TOP_X];

    // choose TOP_X parents from trip
    select( trip, parents );

    // generates TOP_X offsprings from TOP_X parenets
    crossover( parents, offsprings, distanceBtwCities );

    // mutate offsprings
    mutate( offsprings );

    // populate the next generation.
    populate( trip, offsprings );
  }

  // stop a timer
  cout << "elapsed time = " << timer.lap( ) << endl;
  return 0;
}

/*
 * Initializes trip[CHROMOSOMES] with chromosome.txt and coordiantes[CITIES][2] with cities.txt
 *
 * @param trip[CHROMOSOMES]:      50000 different trips
 * @param coordinates[CITIES][2]: (x, y) coordinates of 36 different cities: ABCDEFGHIJKLMNOPQRSTUVWXYZ
 */
void initialize( Trip trip[CHROMOSOMES], int coordinates[CITIES][2] ) {
  // open two files to read chromosomes (i.e., trips)  and cities
  ifstream chromosome_file( "chromosome.txt" );
  ifstream cities_file( "cities.txt" );
  
  // read data from the files
  // chromosome.txt:                                                                                           
  //   T8JHFKM7BO5XWYSQ29IP04DL6NU3ERVA1CZG                                                                    
  //   FWLXU2DRSAQEVYOBCPNI608194ZHJM73GK5T                                                                    
  //   HU93YL0MWAQFIZGNJCRV12TO75BPE84S6KXD
  for ( int i = 0; i < CHROMOSOMES; i++ ) {
    chromosome_file >> trip[i].itinerary;
    trip[i].fitness = 0.0;
  }

  // cities.txt:                                                                                               
  // name    x       y                                                                                         
  // A       83      99                                                                                        
  // B       77      35                                                                                        
  // C       14      64                                                                                        
  for ( int i = 0; i < CITIES; i++ ) {
    char city;
    cities_file >> city;
    int index = ( city >= 'A' ) ? city - 'A' : city - '0' + 26;
    cities_file >> coordinates[index][0] >> coordinates[index][1];
  }

  // close the files.
  chromosome_file.close( );
  cities_file.close( );

  // just for debugging
  if ( DEBUG ) {
    for ( int i = 0; i < CHROMOSOMES; i++ )
      cout << trip[i].itinerary << endl;
    for ( int i = 0; i < CITIES; i++ )
      cout << coordinates[i][0] << "\t" << coordinates[i][1] << endl;
  }
}

/*
 * Select the first TOP_X parents from trip[CHROMOSOMES]
 *
 * @param trip[CHROMOSOMES]: all trips
 * @param parents[TOP_X]:    the firt TOP_X parents
 */
void select( Trip trip[CHROMOSOMES], Trip parents[TOP_X] ) {
  // just copy TOP_X trips to parents
  for ( int i = 0; i < TOP_X; i++ )
    strncpy( parents[i].itinerary, trip[i].itinerary, CITIES + 1 );
}

/*
 * Replace the bottom TOP_X trips with the TOP_X offsprings
 */
void populate( Trip trip[CHROMOSOMES], Trip offsprings[TOP_X] ) {

  // just copy TOP_X offsprings to the bottom TOP_X trips.
  for ( int i = 0; i < TOP_X; i++ )
    strncpy( trip[ CHROMOSOMES - TOP_X + i ].itinerary, offsprings[i].itinerary, CITIES + 1 );

  // for debugging
  if ( DEBUG ) {
    for ( int chrom = 0; chrom < CHROMOSOMES; chrom++ ) 
      cout << "chrom[" << chrom << "] = " << trip[chrom].itinerary 
	   << ", trip distance = " << trip[chrom].fitness << endl;
  }
}
