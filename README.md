# Backend Engineering Challenge

## How to run

```bash
python main.py --input_file events.json --window_size 10
```

## Implementation

- Code implementation was done using Python 3.11.7
- Code was formatted using the black python formatter
- No external packages used

## Thought proccess

To solve this problem we need to calculate the average delivery time inside a sliding window of size n.

Before we can do that we need a list with all the minutes between the first and last timestamp, any minute that is not in our original file has a duration of -1.

So using the input provided and a window size of 3 we have:

```
      [20, -1, -1, -1, 31, -1, -1, -1, ... , 54] <-- Sample data duration
[ , , ]                                          <-- Sliding window of size 3

Average in sliding window: 0
```

on the first itteration we move the slidding window forward so we end up with:

```
1st itterration

      [20, -1, -1, -1, 31, -1, -1, -1, ... , 54] <-- Sample data duration
[  ,  ,20]                                       <-- Sliding window of size 3

Average in sliding window: 20
```

and add 20 to the sliding window, and figure out the average inside the window.

```
2nd itterration

   [20, -1, -1, -1, 31, -1, -1, -1, ... , 54] <-- Sample data duration
[  ,20, -1]                                   <-- Sliding window of size 3

Average in sliding window: 20
```

```
3rd itterration

[20, -1, -1, -1, 31, -1, -1, -1, ... , 54] <-- Sample data duration
[20, -1, -1]                               <-- Sliding window of size 3

Average in sliding window: 20
```

```
4th itterration

[20, -1, -1, -1, 31, -1, -1, -1, ... , 54] <-- Sample data duration
    [-1, -1, -1]                           <-- Sliding window of size 3

Average in sliding window: 0
```

and so on until we reach the last timestamp.

## Assumptions

- Input file will always be valid

## Performance

Inside the code there is a comment with a simple performance test using the timeit python module.

Using my machine I get 1000 executions in 0.5867935860005673 seconds.

Unfortunatly I don't have anything else to compare it against except my first commit without some optimizations wich had a execution time of 1.1433425469986108.


# Original Challenge

Welcome to our Engineering Challenge repository 🖖

If you found this repository it probably means that you are participating in our recruitment process. Thank you for your time and energy. If that's not the case please take a look at our [openings](https://unbabel.com/careers/) and apply!

Please fork this repo before you start working on the challenge, read it careful and take your time and think about the solution. Also, please fork this repository because we will evaluate the code on the fork.

This is an opportunity for us both to work together and get to know each other in a more technical way. If you have any questions please open and issue and we'll reach out to help.

Good luck!

## Challenge Scenario

At Unbabel we deal with a lot of translation data. One of the metrics we use for our clients' SLAs is the delivery time of a translation. 

In the context of this problem, and to keep things simple, our translation flow is going to be modeled as only one event.

### *translation_delivered*

Example:

```json
{
	"timestamp": "2018-12-26 18:12:19.903159",
	"translation_id": "5aa5b2f39f7254a75aa4",
	"source_language": "en",
	"target_language": "fr",
	"client_name": "airliberty",
	"event_name": "translation_delivered",
	"duration": 20,
	"nr_words": 100
}
```

## Challenge Objective

Your mission is to build a simple command line application that parses a stream of events and produces an aggregated output. In this case, we're interested in calculating, for every minute, a moving average of the translation delivery time for the last X minutes.

If we want to count, for each minute, the moving average delivery time of all translations for the past 10 minutes we would call your application like (feel free to name it anything you like!).

	unbabel_cli --input_file events.json --window_size 10
	
The input file format would be something like:

	{"timestamp": "2018-12-26 18:11:08.509654","translation_id": "5aa5b2f39f7254a75aa5","source_language": "en","target_language": "fr","client_name": "airliberty","event_name": "translation_delivered","nr_words": 30, "duration": 20}
	{"timestamp": "2018-12-26 18:15:19.903159","translation_id": "5aa5b2f39f7254a75aa4","source_language": "en","target_language": "fr","client_name": "airliberty","event_name": "translation_delivered","nr_words": 30, "duration": 31}
	{"timestamp": "2018-12-26 18:23:19.903159","translation_id": "5aa5b2f39f7254a75bb3","source_language": "en","target_language": "fr","client_name": "taxi-eats","event_name": "translation_delivered","nr_words": 100, "duration": 54}

Assume that the lines in the input are ordered by the `timestamp` key, from lower (oldest) to higher values, just like in the example input above.

The output file would be something in the following format.

```
{"date": "2018-12-26 18:11:00", "average_delivery_time": 0}
{"date": "2018-12-26 18:12:00", "average_delivery_time": 20}
{"date": "2018-12-26 18:13:00", "average_delivery_time": 20}
{"date": "2018-12-26 18:14:00", "average_delivery_time": 20}
{"date": "2018-12-26 18:15:00", "average_delivery_time": 20}
{"date": "2018-12-26 18:16:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:17:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:18:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:19:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:20:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:21:00", "average_delivery_time": 25.5}
{"date": "2018-12-26 18:22:00", "average_delivery_time": 31}
{"date": "2018-12-26 18:23:00", "average_delivery_time": 31}
{"date": "2018-12-26 18:24:00", "average_delivery_time": 42.5}
```

#### Notes

Before jumping right into implementation we advise you to think about the solution first. We will evaluate, not only if your solution works but also the following aspects:

+ Simple and easy to read code. Remember that [simple is not easy](https://www.infoq.com/presentations/Simple-Made-Easy)
+ Comment your code. The easier it is to understand the complex parts, the faster and more positive the feedback will be
+ Consider the optimizations you can do, given the order of the input lines
+ Include a README.md that briefly describes how to build and run your code, as well as how to **test it**
+ Be consistent in your code. 

Feel free to, in your solution, include some your considerations while doing this challenge. We want you to solve this challenge in the language you feel most comfortable with. Our machines run Python (3.7.x or higher) or Go (1.16.x or higher). If you are thinking of using any other programming language please reach out to us first 🙏.

Also, if you have any problem please **open an issue**. 

Good luck and may the force be with you
