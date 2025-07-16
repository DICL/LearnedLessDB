#pragma once

#include <string>
#include <vector>
#include <deque>
#include "koo/koo.h"
#include "koo/util.h"


// Code modified from https://github.com/RyanMarcus/plr

using leveldb::Slice;
namespace leveldb { class Slice; }

namespace koo {

struct point {
    double x;
    double y;

    point() = default;
    point(double x, double y) : x(x), y(y) {}
};

struct line {
    double a;
    double b;
};

class Segment {
public:
    Segment(uint64_t _x, double _k, double _b,
						uint64_t _x_last, uint32_t _y_last)
			: x(_x), k(_k), b(_b), x_last(_x_last), y_last(_y_last) {}

		Segment(const Segment& copy)
			: x(copy.x), k(copy.k), b(copy.b), 
				x_last(copy.x_last), y_last(copy.y_last) {}

    /*Segment(uint64_t _x, double _k, double _b,
						uint64_t _x_last, uint32_t _y_last, uint64_t _x_eh)
			: x(_x), k(_k), b(_b), x_last(_x_last), y_last(_y_last), x_eh(_x_eh) {}

		Segment(const Segment& copy)
			: x(copy.x), k(copy.k), b(copy.b), 
				x_last(copy.x_last), y_last(copy.y_last), 
				x_eh(copy.x_eh) {}*/

		~Segment() {}

    uint64_t x;		// the first key in the segment (x값, real min_key)
    double k;			// coefficient a
    double b;			// coefficient b		y = ax+b

    uint64_t x_last;		// the last key in the segment (x값, real max_key)
    uint32_t y_last;		// the index of the last key in the segment (y값, 0~keys.size()-1)

		//uint64_t x_eh;		// x_last of previous segment + 1
};

double get_slope(struct point p1, struct point p2);
struct line get_line(struct point p1, struct point p2);
struct point get_intersetction(struct line l1, struct line l2);

bool is_above(struct point pt, struct line l);
bool is_below(struct point pt, struct line l);

struct point get_upper_bound(struct point pt, double gamma);
struct point get_lower_bound(struct point pt, double gamma);


class GreedyPLR {
private:
    std::string state;
    double gamma;
    struct point last_pt;
    //struct point llast_pt;		// KOO
    struct point s0;
    struct point s1;
    struct line rho_lower;
    struct line rho_upper;
    struct point sint;

    void setup();
    Segment current_segment();
    Segment process__(struct point pt);

public:
    GreedyPLR(double gamma);
    Segment process(const struct point& pt);
    Segment finish();
};

class PLR {
private:
    double gamma;
    std::vector<Segment> segments;

public:
    PLR(double gamma);
    //std::vector<Segment>& train(std::vector<std::string>& keys);
    std::vector<Segment>& train(std::vector<uint64_t>& keys);
    //std::vector<Segment>& train(std::vector<Slice>& keys);
//    std::vector<double> predict(std::vector<double> xx);
//    double mae(std::vector<double> y_true, std::vector<double> y_pred);
};

}
