package org.wikipedia;

import junit.framework.Assert;

import org.junit.Test;

public class TestRegex {
	@Test
	public void testRemoveSquareBracket() {
		String src = "Can you[1] help[2] me?";
		String dest = "Can you help me?";
		String actual = src.replaceAll("\\[\\d+\\]", "");
		Assert.assertEquals(dest, actual);
	}
}
