package weibo4j;

import weibo4j.model.Comment;
import weibo4j.model.CommentWapper;
import weibo4j.model.Paging;
import weibo4j.model.PostParameter;
import weibo4j.model.WeiboException;
import weibo4j.org.json.JSONArray;
import weibo4j.util.WeiboConfig;

public class Comments extends Weibo {

  private static final long serialVersionUID = 3321231200237418256L;

  /**
   * 
   * @param id
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a href="http://open.weibo.com/wiki/2/comments/show">comments/show</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentById(String id) throws WeiboException {
    return Comment.constructWapperComments(client.get(
        WeiboConfig.getValue("baseURL") + "comments/show.json",
        new PostParameter[] { new PostParameter("id", id) }));
  }

  /**
   * 
   * @param id
   * @param count
   * @param page
   * @param filter_by_author
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a href="http://open.weibo.com/wiki/2/comments/show">comments/show</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentById(String id, Paging page,
      Integer filter_by_author) throws WeiboException {
    return Comment
        .constructWapperComments(client.get(
            WeiboConfig.getValue("baseURL") + "comments/show.json",
            new PostParameter[] {
                new PostParameter("id", id),
                new PostParameter("filter_by_author", filter_by_author
                    .toString()) }, page));
  }

  /**
   * 
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/by_me">comments/by_me</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentByMe() throws WeiboException {
    return Comment.constructWapperComments(client.get(WeiboConfig
        .getValue("baseURL") + "comments/by_me.json"));
  }

  /**
   * 
   * @param count
   * @param page
   * @param filter_by_source
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/by_me">comments/by_me</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentByMe(Paging page, Integer filter_by_source)
      throws WeiboException {
    return Comment.constructWapperComments(client.get(
        WeiboConfig.getValue("baseURL") + "comments/by_me.json",
        new PostParameter[] { new PostParameter("filter_by_author",
            filter_by_source.toString()) }, page));
  }

  /**
   * 
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/to_me">comments/to_me</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentToMe() throws WeiboException {
    return Comment.constructWapperComments(client.get(WeiboConfig
        .getValue("baseURL") + "comments/to_me.json"));
  }

  /**
   * 
   * @param count
   * @param page
   * @param filter_by_author
   * @param filter_by_source
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/to_me">comments/to_me</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentToMe(Paging page, Integer filter_by_source,
      Integer filter_by_author) throws WeiboException {
    return Comment
        .constructWapperComments(client.get(
            WeiboConfig.getValue("baseURL") + "comments/to_me.json",
            new PostParameter[] {
                new PostParameter("filter_by_source", filter_by_source
                    .toString()),
                new PostParameter("filter_by_author", filter_by_author
                    .toString()) }, page));
  }

  /**
   * 
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/timeline">comments/timeline</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentTimeline() throws WeiboException {
    return Comment.constructWapperComments(client.get(WeiboConfig
        .getValue("baseURL") + "comments/timeline.json"));
  }

  /**
   * 
   * @param count
   * @param page
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/timeline">comments/timeline</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentTimeline(Paging page) throws WeiboException {
    return Comment
        .constructWapperComments(client.get(WeiboConfig.getValue("baseURL")
            + "comments/timeline.json", null, page));
  }

  /**
   * 
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/mentions">comments/mentions</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentMentions() throws WeiboException {
    return Comment.constructWapperComments(client.get(WeiboConfig
        .getValue("baseURL") + "comments/mentions.json"));
  }

  /**
   * 
   * @param count
   * @param page
   * @param filter_by_author
   * @param filter_by_source
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/mentions">comments/mentions</a>
   * @since JDK 1.5
   */
  public CommentWapper getCommentMentions(Paging page,
      Integer filter_by_source, Integer filter_by_author) throws WeiboException {
    return Comment
        .constructWapperComments(client.get(
            WeiboConfig.getValue("baseURL") + "comments/mentions.json",
            new PostParameter[] {
                new PostParameter("filter_by_source", filter_by_source
                    .toString()),
                new PostParameter("filter_by_author", filter_by_author
                    .toString()) }, page));
  }

  /**
   * 
   * @param cids
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/show_batch">comments/show_batch</a>
   * @since JDK 1.5
   */
  public JSONArray getCommentShowBatch(String cids) throws WeiboException {
    return client.get(
        WeiboConfig.getValue("baseURL") + "comments/show_batch.json",
        new PostParameter[] { new PostParameter("cids", cids) }).asJSONArray();
  }

  /**
   * 
   * @param comment
   * @param id
   * @return Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/create">comments/create</a>
   * @since JDK 1.5
   */
  public Comment createComment(String comment, String id) throws WeiboException {
    return new Comment(client.post(WeiboConfig.getValue("baseURL")
        + "comments/create.json", new PostParameter[] {
        new PostParameter("comment", comment), new PostParameter("id", id) }));
  }

  /**
   * 
   * @param comment
   * @param id
   * @param comment_ori
   * @return Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/create">comments/create</a>
   * @since JDK 1.5
   */
  public Comment createComment(String comment, String id, Integer comment_ori)
      throws WeiboException {
    return new Comment(client.post(WeiboConfig.getValue("baseURL")
        + "comments/create.json", new PostParameter[] {
        new PostParameter("comment", comment), new PostParameter("id", id),
        new PostParameter("comment_ori", comment_ori.toString()) }));
  }

  /**
   * 
   * @param comment
   * 
   * @param cid
   * @param id
   * @return Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/reply">comments/reply</a>
   * @since JDK 1.5
   */
  public Comment replyComment(String cid, String id, String comment)
      throws WeiboException {
    return new Comment(client.post(WeiboConfig.getValue("baseURL")
        + "comments/reply.json", new PostParameter[] {
        new PostParameter("cid", cid), new PostParameter("id", id),
        new PostParameter("comment", comment) }));
  }

  /**
   * 
   * @param comment
   * @param cid
   * @param id
   * @param without_mention
   * @param comment_ori
   * @return Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/reply">comments/reply</a>
   * @since JDK 1.5
   */
  public Comment replyComment(String cid, String id, String comment,
      Integer without_mention, Integer comment_ori) throws WeiboException {
    return new Comment(client.post(WeiboConfig.getValue("baseURL")
        + "comments/reply.json", new PostParameter[] {
        new PostParameter("comment", comment), new PostParameter("id", id),
        new PostParameter("cid", cid),
        new PostParameter("without_mention", without_mention.toString()),
        new PostParameter("comment_ori", comment_ori.toString()) }));
  }

  /**
   * 
   * @param cid
   * @return Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/destroy">comments/destroy</a>
   * @since JDK 1.5
   */
  public Comment destroyComment(String cid) throws WeiboException {
    return new Comment(client.post(WeiboConfig.getValue("baseURL")
        + "comments/destroy.json", new PostParameter[] { new PostParameter(
        "cid", cid) }));
  }

  /**
   * 
   * @param ids
   * @return list of Comment
   * @throws WeiboException
   *           when Weibo service or network is unavailable
   * @version weibo4j-V2 1.0.1
   * @see <a
   *      href="http://open.weibo.com/wiki/2/comments/destroy_batch">comments/destroy_batch</a>
   * @since JDK 1.5
   */
  public JSONArray destoryCommentBatch(String cids) throws WeiboException {
    return client.post(
        WeiboConfig.getValue("baseURL") + "comments/destroy_batch.json",
        new PostParameter[] { new PostParameter("cids", cids) }).asJSONArray();
  }
}
