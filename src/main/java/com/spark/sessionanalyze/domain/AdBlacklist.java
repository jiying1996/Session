package com.spark.sessionanalyze.domain;

import java.util.Objects;

/**
 * 广告黑名单
 * @author Administrator
 *
 */
public class AdBlacklist {

	private long userid;

	public long getUserid() {
		return userid;
	}
	public void setUserid(long userid) {
		this.userid = userid;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		AdBlacklist that = (AdBlacklist) o;
		return userid == that.userid;
	}

	@Override
	public int hashCode() {
		return Objects.hash(userid);
	}
}
