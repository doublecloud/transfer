This benchmark result demonstrates the changes after adding multithreading and caching schemes.

The test was run on a computer on Apple M1 Pro (10 cores) processor.

Caching disabled
```
BenchmarkParsingViaMultithreading
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_1_threads-10         	    1021	   	       810.6 messages/s	   1233687 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_2_threads-10         	     994	   	       776.7 messages/s	   1287446 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_4_threads-10         	     909	   	       759.4 messages/s	   1316754 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_8_threads-10         	     919	   	       754.9 messages/s	   1324650 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_16_threads-10        	     970	   	       752.1 messages/s	   1329670 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_32_threads-10        	     890	   	       743.2 messages/s	   1345459 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_64_threads-10        	     814	   	       727.6 messages/s	   1374322 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_1_threads-10        	     291	   	      2445 messages/s	    408958 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_2_threads-10        	     454	   	      3838 messages/s	    260558 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_4_threads-10        	     606	   	      5063 messages/s	    197493 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_8_threads-10        	     687	   	      5813 messages/s	    172024 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_16_threads-10       	     716	   	      5801 messages/s	    172389 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_32_threads-10       	     696	   	      5793 messages/s	    172628 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_64_threads-10       	     688	   	      5761 messages/s	    173590 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_1_threads-10       	      34	  	      2949 messages/s	    339105 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_2_threads-10       	      63	  	      5370 messages/s	    186235 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_4_threads-10       	     100	  	      8726 messages/s	    114594 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_8_threads-10       	     130	   	     10871 messages/s	     91987 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_16_threads-10      	     134	   	     11186 messages/s	     89397 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_32_threads-10      	     134	   	     11157 messages/s	     89633 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_64_threads-10      	     130	   	     11061 messages/s	     90407 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_1_threads-10      	       4	 	      3056 messages/s	    327201 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_2_threads-10      	       6	 	      5668 messages/s	    176427 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_4_threads-10      	      10	 	      9857 messages/s	    101454 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_8_threads-10      	      15	  	     14007 messages/s	     71391 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_16_threads-10     	      16	  	     14736 messages/s	     67863 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_32_threads-10     	      16	  	     14492 messages/s	     69006 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_64_threads-10     	      16	  	     14838 messages/s	     67393 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_1_threads-10     	       1		      3111 messages/s	    321448 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_2_threads-10     	       1		      5869 messages/s	    170385 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_4_threads-10     	       2	 	     10671 messages/s	     93709 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_8_threads-10     	       2	 	     15854 messages/s	     63076 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_16_threads-10    	       2	 	     17118 messages/s	     58417 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_32_threads-10    	       2	 	     17122 messages/s	     58404 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_64_threads-10    	       2	 	     16920 messages/s	     59102 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_1_threads-10    	       1		      3115 messages/s	    321029 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_2_threads-10    	       1		      5878 messages/s	    170130 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_4_threads-10    	       1		     10830 messages/s	     92336 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_8_threads-10    	       1		     16970 messages/s	     58929 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_16_threads-10   	       1		     17835 messages/s	     56070 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_32_threads-10   	       1		     17753 messages/s	     56330 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_64_threads-10   	       1		     17702 messages/s	     56492 ns/message
```
Caching enabled
```
BenchmarkParsingViaMultithreading
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_1_threads-10         	    1090	   	       889.6 messages/s	   1124100 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_2_threads-10         	    1071	   	       891.3 messages/s	   1121983 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_4_threads-10         	    1003	   	       874.4 messages/s	   1143700 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_8_threads-10         	    1048	   	       874.7 messages/s	   1143200 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_16_threads-10        	    1024	   	       871.7 messages/s	   1147223 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_32_threads-10        	    1006	   	       854.7 messages/s	   1170044 ns/message
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_1_messages_in_batch_in_64_threads-10        	    1004	   	       851.7 messages/s	   1174091 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_1_threads-10        	     426	   	      3528 messages/s	    283457 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_2_threads-10        	     600	   	      5039 messages/s	    198465 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_4_threads-10        	     730	   	      6315 messages/s	    158355 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_8_threads-10        	     813	   	      6725 messages/s	    148708 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_16_threads-10       	     799	   	      6895 messages/s	    145042 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_32_threads-10       	     775	   	      6860 messages/s	    145778 ns/message
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_10_messages_in_batch_in_64_threads-10       	     813	   	      6698 messages/s	    149293 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_1_threads-10       	      56	  	      4827 messages/s	    207179 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_2_threads-10       	      99	  	      8490 messages/s	    117782 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_4_threads-10       	     171	   	     14379 messages/s	     69544 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_8_threads-10       	     217	   	     18403 messages/s	     54339 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_16_threads-10      	     225	   	     18563 messages/s	     53871 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_32_threads-10      	     223	   	     18830 messages/s	     53106 ns/message
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_100_messages_in_batch_in_64_threads-10      	     225	   	     18919 messages/s	     52857 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_1_threads-10      	       6	 	      5149 messages/s	    194203 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_2_threads-10      	      10	 	      9520 messages/s	    105044 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_4_threads-10      	      19	  	     16593 messages/s	     60268 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_8_threads-10      	      28	  	     24363 messages/s	     41047 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_16_threads-10     	      30	  	     26028 messages/s	     38420 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_32_threads-10     	      30	  	     25636 messages/s	     39008 ns/message
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_1000_messages_in_batch_in_64_threads-10     	      30	  	     26204 messages/s	     38162 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_1_threads-10     	       1		      5148 messages/s	    194246 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_2_threads-10     	       1		      9703 messages/s	    103059 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_4_threads-10     	       2	 	     17308 messages/s	     57776 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_8_threads-10     	       3	 	     26205 messages/s	     38161 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_16_threads-10    	       3	 	     28505 messages/s	     35081 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_32_threads-10    	       3	 	     29372 messages/s	     34046 ns/message
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_10000_messages_in_batch_in_64_threads-10    	       3	 	     29367 messages/s	     34052 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_1_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_1_threads-10    	       1		      5234 messages/s	    191042 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_2_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_2_threads-10    	       1		      9801 messages/s	    102030 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_4_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_4_threads-10    	       1		     17903 messages/s	     55857 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_8_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_8_threads-10    	       1		     27613 messages/s	     36215 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_16_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_16_threads-10   	       1		     30230 messages/s	     33079 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_32_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_32_threads-10   	       1		     31186 messages/s	     32065 ns/message
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_64_threads
BenchmarkParsingViaMultithreading/process_100000_messages_in_batch_in_64_threads-10   	       1		     30647 messages/s	     32630 ns/message
```
Thus, adding schema caching and multithreading speeds up parsing.
