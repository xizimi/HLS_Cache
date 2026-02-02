#include <iostream>
#include <vector>
#include <queue>
#include <cmath>
#include <tuple>
#include <algorithm>
using namespace std;

// 计算两点之间的距离
double dist(pair<double, double> a, pair<double, double> b) {
    double dx = a.first - b.first;
    double dy = a.second - b.second;
    return sqrt(dx * dx + dy * dy);
}

int main() {
    int T;
    cin >> T;
    while (T--) {
        int n, e, p;
        double r1, r2;
        cin >> n >> e >> p >> r1 >> r2;
        vector<pair<double, double>> torches(n + 1); // 火炬坐标，1-indexed
        for (int i = 1; i <= n; i++) {
            cin >> torches[i].first >> torches[i].second;
        }
        // 状态：(火炬编号, 是否使用过龙血精华, 剩余爆发次数)
        // 值：(药剂数量, 精华使用次数)
        // 使用优先队列进行BFS/Dijkstra
        // 优先级：先比较精华次数，再比较药剂数量
        const int INF = 1e9;
        // dp[i][used][remain] = 到达火炬i，精华使用状态used，剩余爆发次数remain时的最小药剂数
        vector<vector<vector<int>>> dp(n + 1, 
            vector<vector<int>>(2, vector<int>(p + 1, INF)));
        // 优先队列：(精华次数, 药剂数, 火炬编号, 是否用过精华, 剩余爆发次数)
        priority_queue<tuple<int, int, int, int, int>, 
                       vector<tuple<int, int, int, int, int>>,
                       greater<tuple<int, int, int, int, int>>> pq;
        // 初始状态：在火炬1，未使用精华，剩余0次爆发
        dp[1][0][0] = 0;
        pq.push({0, 0, 1, 0, 0});
        while (!pq.empty()) {
            auto [essence_used, potions, curr, used_essence, burst_left] = pq.top();
            pq.pop();
            // 如果已经有更好的方案，跳过
            if (dp[curr][used_essence][burst_left] < potions) continue;
            // 到达目标
            if (curr == n) continue;
            // 尝试跳到其他火炬
            for (int next = 1; next <= n; next++) {
                if (next == curr) continue;
                double d = dist(torches[curr], torches[next]);
                // 情况1：使用普通冲刺（距离<=r1）
                if (d <= r1) {
                    int new_potions = potions + 1;
                    int new_burst = (burst_left > 0) ? burst_left - 1 : 0;
                    
                    if (dp[next][used_essence][new_burst] > new_potions) {
                        dp[next][used_essence][new_burst] = new_potions;
                        pq.push({essence_used, new_potions, next, used_essence, new_burst});
                    }
                }
                // 情况2：在爆发状态下冲刺（距离<=r2，且有剩余爆发次数）
                if (burst_left > 0 && d <= r2) {
                    int new_potions = potions + 1;
                    int new_burst = burst_left - 1;
                    
                    if (dp[next][used_essence][new_burst] > new_potions) {
                        dp[next][used_essence][new_burst] = new_potions;
                        pq.push({essence_used, new_potions, next, used_essence, new_burst});
                    }
                }
                // 情况3：使用龙血精华激活爆发（未使用过精华，且距离<=r2）
                if (used_essence == 0 && d <= r2) {
                    int new_potions = potions + 1;
                    int new_essence = 1;
                    int new_burst = p - 1; // 使用了1次，剩余p-1次
                    
                    if (dp[next][1][new_burst] > new_potions) {
                        dp[next][1][new_burst] = new_potions;
                        pq.push({new_essence, new_potions, next, 1, new_burst});
                    }
                }
            }
        }
        // 找到到达火炬n的最优方案
        int min_essence = 1, min_potions = INF;
        // 优先找不使用精华的方案
        for (int remain = 0; remain <= p; remain++) {
            if (dp[n][0][remain] < INF) {
                min_essence = 0;
                min_potions = min(min_potions, dp[n][0][remain]);
            }
        }
        // 如果没有不用精华的方案，找使用精华的方案
        if (min_essence == 1) {
            for (int remain = 0; remain <= p; remain++) {
                if (dp[n][1][remain] < INF) {
                    min_potions = min(min_potions, dp[n][1][remain]);
                }
            }
        }
        cout << min_essence << " " << min_potions << endl;
    }
    return 0;
}