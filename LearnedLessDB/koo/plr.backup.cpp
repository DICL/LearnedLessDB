#include "koo/plr.h"
#include <algorithm>
#include <iostream>
#include <string>
#include <vector>
#include <math.h>


// Code modified from https://github.com/RyanMarcus/plr

namespace koo {

double 
get_slope(struct point p1, struct point p2) {
    return (p2.y - p1.y) / (p2.x - p1.x);
}

struct line
get_line(struct point p1, struct point p2) {
    double a = get_slope(p1, p2);
    double b = -a * p1.x + p1.y;
    struct line l{.a = a, .b = b};
    return l;
}

struct point
get_intersetction(struct line l1, struct line l2) {
    double a = l1.a;
    double b = l2.a;
    double c = l1.b;
    double d = l2.b;
    struct point p {(d - c) / (a - b), (a * d - b * c) / (a - b)};
    return p;
}

bool
is_above(struct point pt, struct line l) {
    return pt.y > l.a * pt.x + l.b;
}

bool
is_below(struct point pt, struct line l) {
    return pt.y < l.a * pt.x + l.b;
}

struct point
get_upper_bound(struct point pt, double gamma) {
    struct point p {pt.x, pt.y + gamma};
    return p;
}

struct point
get_lower_bound(struct point pt, double gamma) {
    struct point p {pt.x, pt.y - gamma};
    return p;
}

GreedyPLR::GreedyPLR(double gamma) {
    this->state = "need2";
    this->gamma = gamma;
}

int counter = 0;

Segment
GreedyPLR::process(const struct point& pt) {
    Segment s = {0, 0, 0, 0, 0};
    if (this->state.compare("need2") == 0) {
        this->s0 = pt;
        this->state = "need1";
    } else if (this->state.compare("need1") == 0) {
        this->s1 = pt;
        setup();
        this->state = "ready";
    } else if (this->state.compare("ready") == 0) {
        s = process__(pt);
    } else {
        // impossible
        std::cout << "ERROR in process" << std::endl;
    }
    //this->llast_pt = this->last_pt;
    this->last_pt = pt;
    return s;
}

void
GreedyPLR::setup() {
    this->rho_lower = get_line(get_upper_bound(this->s0, this->gamma),
                               get_lower_bound(this->s1, this->gamma));
    this->rho_upper = get_line(get_lower_bound(this->s0, this->gamma),
                               get_upper_bound(this->s1, this->gamma));
    this->sint = get_intersetction(this->rho_upper, this->rho_lower);
}

Segment
GreedyPLR::current_segment() {
    uint64_t segment_start = this->s0.x;
    double avg_slope = (this->rho_lower.a + this->rho_upper.a) / 2.0;
    double intercept = -avg_slope * this->sint.x + this->sint.y;

		uint64_t x_last = this->last_pt.x;
		uint32_t y_last = this->last_pt.y;

    Segment s = {segment_start, avg_slope, intercept, x_last, y_last};
    return s;
}

Segment
GreedyPLR::process__(struct point pt) {
    if (!(is_above(pt, this->rho_lower) && is_below(pt, this->rho_upper))) {
      // new point out of error bounds
        Segment prev_segment = current_segment();
        /*if (!file && (u_int32_t) pt.y % 2 == 1) {			level model만 해당
          // current point is the largest point in the segments
            this->s0 = last_pt;
            this->s1 = pt;
            setup();
            this->state = "ready";

            prev_segment.x_last = llast_pt.x;
            prev_segment.y_last = llast_pt.y;

            return prev_segment;
        } else {*/
            this->s0 = pt;
            this->state = "need1";
        //}
        return prev_segment;
    }

    struct point s_upper = get_upper_bound(pt, this->gamma);
    struct point s_lower = get_lower_bound(pt, this->gamma);
    if (is_below(s_upper, this->rho_upper)) {
        this->rho_upper = get_line(this->sint, s_upper);
    }
    if (is_above(s_lower, this->rho_lower)) {
        this->rho_lower = get_line(this->sint, s_lower);
    }
    Segment s = {0, 0, 0, 0, 0};
    return s;
}

Segment
GreedyPLR::finish() {
    Segment s = {0, 0, 0, 0, 0};
    if (this->state.compare("need2") == 0) {
        this->state = "finished";
        return s;
    } else if (this->state.compare("need1") == 0) {
        this->state = "finished";
        s.x = this->s0.x;
        s.k = 0;
        s.b = this->s0.y;

        s.x_last = this->last_pt.x;
        s.y_last = this->last_pt.y;

        return s;
    } else if (this->state.compare("ready") == 0) {
        this->state = "finished";
        return current_segment();
    } else {
        std::cout << "ERROR in finish" << std::endl;
        return s;
    }
}

PLR::PLR(double gamma) {
    this->gamma = gamma;
}

std::vector<Segment>&
//PLR::train(std::vector<std::string>& keys) {
PLR::train(std::vector<uint64_t>& keys) {
//PLR::train(std::vector<Slice>& keys) {
    GreedyPLR plr(this->gamma);
    int count = 0;
    size_t size = keys.size();
    //bool notEmpty = false;
    uint64_t x = keys[0];
    for (int i = 0; i < size; ++i) {
        //Segment seg = plr.process(point((double) std::stoull(keys[i]), i));
        Segment seg = plr.process(point((double) keys[i], i));
        if (seg.x != 0 ||
            seg.k != 0 ||
            seg.b != 0) {
#if DEBUG
						// Learning error? x=x_last, k=-nan, b=nan 인 선분 생성됨
						// 그냥 앞 선분에 붙이자
						if (std::isnan(seg.b)) {			// KOO
							std::cout << "NAN\n";
						} else {				// 원래 Bourbon*/
	            this->segments.push_back(seg);
						}
						//notEmpty = true;
#else
						if (!std::isnan(seg.k)) {
							seg.x = x;
							seg.x_last = keys[i-1];
							if (std::isinf(seg.k)) {
								//std::cout << "INF segment\n";
								//std::cout << "(" << seg.x << ", )~(" << seg.x_last << ", " << seg.y_last << "): y = ";
								//std::cout << seg.k << " * x + " << seg.b << std::endl;
								uint64_t y = 0;
								if (this->segments.size()) y = this->segments.back().y_last + 1;
								double dx = static_cast<double>(seg.x_last - seg.x);
								double dy = static_cast<double>(seg.y_last - y);
								seg.k = dy / dx;
								seg.b = static_cast<double>(y) - seg.k * static_cast<double>(seg.x);
								//if (std::isnan(seg.k) || std::isinf(seg.k)) std::cout << "Nooooooooooo!!!!!!!!!!!!\n";
								//if (seg.y_last - y > 8) std::cout << "Noooooooooo?????????\n";
								//std::cout << "(" << seg.x << ", " << y << ")~(" << seg.x_last << ", " << seg.y_last << "): y = ";
								//std::cout << seg.k << " * x + " << seg.b << "\n\n";
							}
			        this->segments.push_back(seg);
							x = keys[i];
						}
						/*else std::cout << "NAN\n";
						std::ofstream ofs("/koo/HyperLearningless3/koo/data/plr_test.txt", std::ios::app);
						ofs << "x: " << seg.x << ", x_last: " << seg.x_last << ", y_last: " << seg.y_last << ", k: " << seg.k << ", b: " << seg.b << std::endl;
						ofs.close();*/
#endif
        }

        /*if (!file && ++count % 10 == 0 && adgMod::env->compaction_awaiting.load() != 0) {		// level model만 해당
            segments.clear();
            return segments;
        }*/
    }

    Segment last = plr.finish();
    if (last.x != 0 ||
        last.k != 0 ||
        last.b != 0) {
				last.x = x;
				last.x_last = keys[size-1];
        this->segments.push_back(last);
				/*std::ofstream ofs("/koo/HyperLearningless3/koo/data/plr_test.txt", std::ios::app);
				ofs << "x: " << last.x << ", x_last: " << last.x_last << ", k: " << last.k << ", b: " << last.b << std::endl;
				ofs.close();*/
    }

    return this->segments;
}

}
